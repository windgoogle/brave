package brave.spring.messaging;

import brave.Span;
import brave.Tracing;
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
    MessageHeaderAccessor headers = MessageHeaderAccessor.getMutableAccessor(message);
    startProducerSpan(headers);
    return new GenericMessage<>(message.getPayload(), headers.getMessageHeaders());
  }

  @Override
  public void afterSendCompletion(Message<?> message, MessageChannel channel, boolean sent,
      Exception ex) {
    finishSpan(Span.Kind.PRODUCER, ex);
  }

  @Override public Message<?> postReceive(Message<?> message, MessageChannel channel) {
    MessageHeaderAccessor headers = MessageHeaderAccessor.getMutableAccessor(message);
    startConsumerSpan(headers);
    return new GenericMessage<>(message.getPayload(), headers.getMessageHeaders());
  }

  @Override
  public void afterReceiveCompletion(Message<?> message, MessageChannel channel, Exception ex) {
    finishSpan(Span.Kind.CONSUMER, ex);
  }

  @Override public Message<?> beforeHandle(Message<?> message, MessageChannel channel,
      MessageHandler handler) {
    MessageHeaderAccessor headers = MessageHeaderAccessor.getMutableAccessor(message);
    startConsumerSpan(headers);
    return new GenericMessage<>(message.getPayload(), headers.getMessageHeaders());
  }

  @Override public void afterMessageHandled(Message<?> message, MessageChannel channel,
      MessageHandler handler, Exception ex) {
    finishSpan(Span.Kind.CONSUMER, ex);
  }

  /** This starts a producer span as a child of the current trace context */
  void startProducerSpan(MessageHeaderAccessor headers) {
    Span span = threadLocalProducerSpan.next();
    MessageHeaderPropagation.removeAnyTraceHeaders(headers, tracing.propagation().keys());
    injector.inject(span.context(), headers);
    if (!span.isNoop()) span.kind(Span.Kind.PRODUCER).start();
    // TODO topic etc
  }

  /** This starts a consumer span as a child of the incoming message or the current trace context */
  void startConsumerSpan(MessageHeaderAccessor headers) {
    TraceContextOrSamplingFlags extracted = extractor.extract(headers);
    Span span = threadLocalConsumerSpan.next(extracted);

    MessageHeaderPropagation.removeAnyTraceHeaders(headers, tracing.propagation().keys());
    injector.inject(span.context(), headers);
    if (!span.isNoop()) span.kind(Span.Kind.CONSUMER).start();
    // TODO topic etc
  }

  void finishSpan(Span.Kind kind, Exception error) {
    Span span = threadLocalSpan(kind).remove();
    if (span == null || span.isNoop()) return;
    if (error != null) { // an error occurred, adding error to span
      String message = error.getMessage();
      if (message == null) message = error.getClass().getSimpleName();
      span.tag("error", message);
    }
    span.finish();
  }

  ThreadLocalSpan threadLocalSpan(Span.Kind kind) {
    return kind == Span.Kind.CONSUMER ? threadLocalConsumerSpan : threadLocalProducerSpan;
  }
}
