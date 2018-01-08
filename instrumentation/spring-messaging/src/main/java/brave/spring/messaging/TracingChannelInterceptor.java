package brave.spring.messaging;

import brave.Span;
import brave.Tracing;
import brave.internal.Nullable;
import brave.propagation.ThreadLocalSpan;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.ChannelInterceptorAdapter;
import org.springframework.messaging.support.ExecutorChannelInterceptor;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.messaging.support.NativeMessageHeaderAccessor;

import static org.springframework.messaging.support.MessageHeaderAccessor.getAccessor;
import static org.springframework.messaging.support.NativeMessageHeaderAccessor.NATIVE_HEADERS;

public final class TracingChannelInterceptor extends ChannelInterceptorAdapter implements
    ExecutorChannelInterceptor {

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
    removeAnyTraceHeaders(headers);
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
    MessageHeaderAccessor headers = getAccessor(message, MessageHeaderAccessor.class);
    TraceContextOrSamplingFlags extracted = extractor.extract(headers);
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

  void removeAnyTraceHeaders(MessageHeaderAccessor accessor) {
    for (String keyToRemove : tracing.propagation().keys()) {
      accessor.removeHeader(keyToRemove);
      if (accessor instanceof NativeMessageHeaderAccessor) {
        NativeMessageHeaderAccessor nativeAccessor = (NativeMessageHeaderAccessor) accessor;
        nativeAccessor.removeNativeHeader(keyToRemove);
      } else {
        Map<String, List<String>> nativeHeaders = (Map) accessor.getHeader(NATIVE_HEADERS);
        if (nativeHeaders == null) continue;
        nativeHeaders.remove(keyToRemove);
      }
    }
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
