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
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.ChannelInterceptorAdapter;
import org.springframework.messaging.support.ExecutorChannelInterceptor;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageHeaderAccessor;

/**
 * This implementation uses {@link ThreadLocalSpan} to propagate context between callbacks. This
 * is an alternative to {@code ThreadStatePropagationChannelInterceptor} which is less sensitive
 * to message manipulation by other interceptors.
 */
public final class TracingChannelInterceptor extends ChannelInterceptorAdapter implements
    ExecutorChannelInterceptor {

  public static ChannelInterceptor create(Tracing tracing) {
    return new TracingChannelInterceptor(tracing);
  }

  final Tracing tracing;
  final ThreadLocalSpan threadLocalProducerSpan;
  final ThreadLocalSpan threadLocalConsumerSpan;
  final TraceContext.Injector<MessageHeaderAccessor> injector;
  final TraceContext.Extractor<MessageHeaderAccessor> extractor;

  @Autowired TracingChannelInterceptor(Tracing tracing) {
    this.tracing = tracing;
    this.threadLocalProducerSpan = ThreadLocalSpan.create(tracing.tracer());
    this.threadLocalConsumerSpan = ThreadLocalSpan.create(tracing.tracer());
    this.injector = tracing.propagation().injector(MessageHeaderPropagation.INSTANCE);
    this.extractor = tracing.propagation().extractor(MessageHeaderPropagation.INSTANCE);
  }

  @Override public Message<?> preSend(Message<?> message, MessageChannel channel) {
    Span span = threadLocalProducerSpan.next(); // removed in afterSendCompletion
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
    finish(threadLocalProducerSpan, ex);
  }

  @Override public Message<?> postReceive(Message<?> message, MessageChannel channel) {
    MessageHeaderAccessor headers = MessageHeaderAccessor.getMutableAccessor(message);
    TraceContextOrSamplingFlags extracted = extractor.extract(headers);
    Span span = threadLocalConsumerSpan.next(extracted); // removed in afterReceiveCompletion

    // replace the headers with the consumer span
    MessageHeaderPropagation.removeAnyTraceHeaders(headers, tracing.propagation().keys());
    injector.inject(span.context(), headers);

    if (!span.isNoop()) span.kind(Span.Kind.CONSUMER).start();
    // TODO topic etc
    return new GenericMessage<>(message.getPayload(), headers.getMessageHeaders());
  }

  @Override
  public void afterReceiveCompletion(Message<?> message, MessageChannel channel, Exception ex) {
    finish(threadLocalConsumerSpan, ex);
  }

  /**
   * Similar to {@link #postReceive(Message, MessageChannel)}, except it doesn't need to inject the
   * consumer span back as message headers. Instead, the headers are cleared to eliminate ambiguity.
   */
  @Override public Message<?> beforeHandle(Message<?> message, MessageChannel channel,
      MessageHandler handler) {
    MessageHeaderAccessor headers = MessageHeaderAccessor.getMutableAccessor(message);
    TraceContextOrSamplingFlags extracted = extractor.extract(headers);
    Span span = threadLocalConsumerSpan.next(extracted); // removed in afterMessageHandled

    // replace the headers with the consumer span
    MessageHeaderPropagation.removeAnyTraceHeaders(headers, tracing.propagation().keys());

    if (!span.isNoop()) span.kind(Span.Kind.CONSUMER).start();
    // TODO topic etc
    return new GenericMessage<>(message.getPayload(), headers.getMessageHeaders());
  }

  @Override public void afterMessageHandled(Message<?> message, MessageChannel channel,
      MessageHandler handler, Exception ex) {
    finish(threadLocalConsumerSpan, ex);
  }

  static void finish(ThreadLocalSpan threadLocalSpan, @Nullable Throwable error) {
    Span span = threadLocalSpan.remove();
    if (span == null || span.isNoop()) return;
    if (error != null) { // an error occurred, adding error to span
      String message = error.getMessage();
      if (message == null) message = error.getClass().getSimpleName();
      span.tag("error", message);
    }
    span.finish();
  }
}
