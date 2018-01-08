package brave.spring.messaging;

import brave.Span;
import brave.Tracing;
import brave.internal.Nullable;
import brave.propagation.Propagation;
import brave.propagation.Propagation.Setter;
import brave.propagation.ThreadLocalSpan;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.ChannelInterceptorAdapter;
import org.springframework.messaging.support.ExecutorChannelInterceptor;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.messaging.support.NativeMessageHeaderAccessor;

public final class TracingChannelInterceptor extends ChannelInterceptorAdapter implements
    ExecutorChannelInterceptor {
  static final Setter<MessageHeaderAccessor, String> SETTER =
      new Setter<MessageHeaderAccessor, String>() {
        @Override public void put(MessageHeaderAccessor accessor, String key, String value) {
          if (accessor instanceof NativeMessageHeaderAccessor) {
            NativeMessageHeaderAccessor nativeAccessor = (NativeMessageHeaderAccessor) accessor;
            nativeAccessor.setNativeHeader(key, value);
          } else {
            accessor.setHeader(key, value);
          }
        }

        @Override public String toString() {
          return "NativeMessageHeaderAccessor::setNativeHeader or MessageHeaderAccessor::setHeader";
        }
      };
  static final Propagation.Getter<MessageHeaders, String> GETTER =
      new Propagation.Getter<MessageHeaders, String>() {
        @Override public String get(MessageHeaders carrier, String key) {
          Object result = carrier.get(key);
          return result != null ? result.toString() : null;
        }

        @Override public String toString() {
          return "MessageHeaders::get";
        }
      };

  public static ChannelInterceptor create(Tracing httpTracing) {
    return new TracingChannelInterceptor(httpTracing);
  }

  final Tracing tracing;
  final ThreadLocalSpan threadLocalSpan;
  final TraceContext.Injector<MessageHeaderAccessor> injector;
  final TraceContext.Extractor<MessageHeaders> extractor;

  @Autowired TracingChannelInterceptor(Tracing tracing) {
    this.tracing = tracing;
    this.threadLocalSpan = ThreadLocalSpan.create(tracing.tracer());
    this.injector = tracing.propagation().injector(SETTER);
    this.extractor = tracing.propagation().extractor(GETTER);
  }

  @Override public Message<?> preSend(Message<?> message, MessageChannel channel) {
    Span span = threadLocalSpan.next(); // this sets the span in scope until afterSendCompletion
    MessageHeaderAccessor headers = MessageHeaderAccessor.getMutableAccessor(message);
    tracing.propagation().keys().forEach(headers::removeHeader);
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

  @Override public Message<?> beforeHandle(Message<?> message, MessageChannel messageChannel,
      MessageHandler messageHandler) {
    TraceContextOrSamplingFlags extracted = extractor.extract(message.getHeaders());
    Span span = threadLocalSpan.next(extracted);// this sets the span in scope until afterHandle
    if (!span.isNoop()) span.kind(Span.Kind.CONSUMER).start();
    // TODO topic etc
    return message;
  }

  @Override public void afterMessageHandled(Message<?> message, MessageChannel messageChannel,
      MessageHandler messageHandler, Exception e) {
    Span span = threadLocalSpan.remove();
    if (span == null || span.isNoop()) return;
    finish(span, e);
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
