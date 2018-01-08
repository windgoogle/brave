package brave.spring.messaging;

import brave.Span;
import brave.Tracing;
import brave.internal.Nullable;
import brave.propagation.ThreadLocalSpan;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.ChannelInterceptorAdapter;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageHeaderAccessor;

public final class TracingChannelInterceptor extends ChannelInterceptorAdapter {

  public static ChannelInterceptor create(Tracing httpTracing) {
    return new TracingChannelInterceptor(httpTracing);
  }

  final Tracing tracing;
  final ThreadLocalSpan threadLocalSpan;
  final TraceContext.Injector<MessageHeaderAccessor> injector;
  final TraceContext.Extractor<MessageHeaderAccessor> extractor;

  @Autowired TracingChannelInterceptor(Tracing tracing) {
    this.tracing = tracing;
    this.threadLocalSpan = ThreadLocalSpan.create(tracing.tracer());
    this.injector = tracing.propagation().injector(MessageHeaderPropagation.INSTANCE);
    this.extractor = tracing.propagation().extractor(MessageHeaderPropagation.INSTANCE);
  }

  @Override public Message<?> preSend(Message<?> message, MessageChannel channel) {
    Span span = threadLocalSpan.next(); // this sets the span in scope until afterSendCompletion
    MessageHeaderAccessor headers = MessageHeaderAccessor.getMutableAccessor(message);
    MessageHeaderPropagation.removeAnyTraceHeaders(headers, tracing.propagation().keys());
    injector.inject(span.context(), headers);
    if (!span.isNoop()) span.kind(Span.Kind.PRODUCER).start();
    // TODO topic etc
    return new GenericMessage<>(message.getPayload(), headers.getMessageHeaders());
  }

  @Override
  public void afterSendCompletion(Message<?> message, MessageChannel channel, boolean sent,
      Exception ex) {
    Span span = threadLocalSpan.remove();
    if (span == null || span.isNoop()) return;
    finish(span, ex);
  }

  @Override public Message<?> postReceive(Message<?> message, MessageChannel channel) {
    MessageHeaderAccessor headers = MessageHeaderAccessor.getMutableAccessor(message);
    TraceContextOrSamplingFlags extracted = extractor.extract(headers);
    Span span = threadLocalSpan.next(extracted); // this sets the span in scope until afterHandle

    // replace the headers with the consumer span
    MessageHeaderPropagation.removeAnyTraceHeaders(headers, tracing.propagation().keys());
    injector.inject(span.context(), headers);

    if (!span.isNoop()) span.kind(Span.Kind.CONSUMER).start();
    // TODO topic etc
    return new GenericMessage<>(message.getPayload(), headers.getMessageHeaders());
  }

  @Override
  public void afterReceiveCompletion(Message<?> message, MessageChannel channel, Exception ex) {
    Span span = threadLocalSpan.remove();
    if (span == null || span.isNoop()) return;
    finish(span, ex);
  }

  static void finish(Span span, @Nullable Throwable error) {
    if (error != null) { // an error occurred, adding error to span
      String message = error.getMessage();
      if (message == null) message = error.getClass().getSimpleName();
      span.tag("error", message);
    }
    span.finish();
  }
}
