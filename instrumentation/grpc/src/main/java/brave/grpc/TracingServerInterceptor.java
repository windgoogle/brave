package brave.grpc;

import brave.Span;
import brave.Tracer;
import brave.propagation.Propagation;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContextOrSamplingFlags;
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import static brave.grpc.GrpcPropagation.RPC_METHOD;

// not exposed directly as implementation notably changes between versions 1.2 and 1.3
final class TracingServerInterceptor implements ServerInterceptor {
  static final Propagation.Getter<Metadata, Key<String>> GETTER =
      new Propagation.Getter<Metadata, Key<String>>() { // retrolambda no like
        @Override public String get(Metadata metadata, Key<String> key) {
          return metadata.get(key);
        }

        @Override public String toString() {
          return "Metadata::get";
        }
      };

  final Tracer tracer;
  final Extractor<Metadata> extractor;
  final GrpcServerParser parser;

  TracingServerInterceptor(GrpcTracing grpcTracing) {
    tracer = grpcTracing.tracing.tracer();
    extractor = grpcTracing.propagation.extractor(GETTER);
    parser = grpcTracing.serverParser;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
      final Metadata headers, final ServerCallHandler<ReqT, RespT> next) {
    TraceContextOrSamplingFlags extracted = extractor.extract(headers);
    Span span = extracted.context() != null
        ? tracer.joinSpan(extracted.context())
        : tracer.nextSpan(extracted);

    // If grpc propagation is enabled, make sure we refresh the server method
    GrpcPropagation.Tags tags = GrpcPropagation.findTags(span.context());
    if (tags != null) {
      tags.put(RPC_METHOD, call.getMethodDescriptor().getFullMethodName());
    }

    span.kind(Span.Kind.SERVER);
    parser.onStart(call, headers, span.customizer());
    // startCall invokes user interceptors, so we place the span in scope here
    ServerCall.Listener<ReqT> result;
    try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
      result = next.startCall(new TracingServerCall<>(span, call, parser), headers);
    } catch (RuntimeException | Error e) {
      span.error(e);
      span.finish();
      throw e;
    }

    // This ensures the server implementation can see the span in scope
    return new ScopingServerCallListener<>(tracer, span, result, parser);
  }

  static final class TracingServerCall<ReqT, RespT>
      extends SimpleForwardingServerCall<ReqT, RespT> {
    final Span span;
    final GrpcServerParser parser;

    TracingServerCall(Span span, ServerCall<ReqT, RespT> call, GrpcServerParser parser) {
      super(call);
      this.span = span;
      this.parser = parser;
    }

    @Override public void request(int numMessages) {
      span.start();
      super.request(numMessages);
    }

    @Override
    public void sendMessage(RespT message) {
      super.sendMessage(message);
      parser.onMessageSent(message, span.customizer());
    }

    @Override public void close(Status status, Metadata trailers) {
      try {
        super.close(status, trailers);
        parser.onClose(status, trailers, span.customizer());
      } catch (RuntimeException | Error e) {
        span.error(e);
        throw e;
      } finally {
        span.finish();
      }
    }
  }

  static final class ScopingServerCallListener<ReqT>
      extends SimpleForwardingServerCallListener<ReqT> {
    final Tracer tracer;
    final Span span;
    final GrpcServerParser parser;

    ScopingServerCallListener(Tracer tracer, Span span, ServerCall.Listener<ReqT> delegate,
        GrpcServerParser parser) {
      super(delegate);
      this.tracer = tracer;
      this.span = span;
      this.parser = parser;
    }

    @Override public void onMessage(ReqT message) {
      try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
        parser.onMessageReceived(message, span.customizer());
        delegate().onMessage(message);
      }
    }

    @Override public void onHalfClose() {
      try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
        delegate().onHalfClose();
      }
    }

    @Override public void onCancel() {
      try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
        delegate().onCancel();
      }
    }

    @Override public void onComplete() {
      try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
        delegate().onComplete();
      }
    }

    @Override public void onReady() {
      try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
        delegate().onReady();
      }
    }
  }
}
