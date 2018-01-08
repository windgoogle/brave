package brave.spring.messaging;

import brave.Tracing;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.ChannelInterceptorAdapter;
import org.springframework.messaging.support.ExecutorSubscribableChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.messaging.support.NativeMessageHeaderAccessor;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.messaging.support.NativeMessageHeaderAccessor.NATIVE_HEADERS;

public class TracingChannelInterceptorTest {

  List<Span> spans = new ArrayList<>();
  ChannelInterceptor interceptor = TracingChannelInterceptor.create(Tracing.newBuilder()
      .spanReporter(spans::add)
      .build());

  List<Message<?>> messages = new ArrayList<>();
  ExecutorSubscribableChannel channel = new ExecutorSubscribableChannel();

  @Test public void injectsProducerSpan() {
    channel.addInterceptor(producerSideOnly(interceptor));
    channel.subscribe(messages::add);

    channel.send(MessageBuilder.withPayload("foo").build());

    assertThat(messages)
        .hasSize(1)
        .flatExtracting(s -> s.getHeaders().keySet())
        .contains("id", "X-B3-TraceId", "X-B3-SpanId", "X-B3-Sampled");
    assertThat(spans)
        .hasSize(1)
        .flatExtracting(Span::kind)
        .containsExactly(Span.Kind.PRODUCER);
  }

  @Test public void injectsProducerSpan_nativeHeaders() {
    channel.addInterceptor(producerSideOnly(interceptor));
    channel.subscribe(messages::add);

    channel.send(MessageBuilder.withPayload("foo").build());

    assertThat(messages)
        .hasSize(1)
        .flatExtracting(s -> ((Map) s.getHeaders().get(NATIVE_HEADERS)).keySet())
        .containsExactly("X-B3-TraceId", "X-B3-SpanId", "X-B3-Sampled");
  }

  @Test public void producerRemovesOldSpanIds() {
    channel.addInterceptor(producerSideOnly(interceptor));
    channel.subscribe(messages::add);

    channel.send(MessageBuilder.withPayload("foo")
        .setHeader("X-B3-TraceId", "a")
        .setHeader("X-B3-ParentSpanId", "a")
        .setHeader("X-B3-SpanId", "a")
        .build());

    assertThat(messages)
        .hasSize(1)
        .flatExtracting(s -> s.getHeaders().keySet())
        .doesNotContain("X-B3-ParentSpanId");
    assertThat(messages)
        .hasSize(1)
        .flatExtracting(s -> s.getHeaders().values())
        .doesNotContain("a");
  }

  @Test public void producerRemovesOldSpanIds_nativeHeaders() {
    channel.addInterceptor(producerSideOnly(interceptor));
    channel.subscribe(messages::add);

    NativeMessageHeaderAccessor accessor = new NativeMessageHeaderAccessor() {
    };

    accessor.setNativeHeader("X-B3-TraceId", "a");
    accessor.setNativeHeader("X-B3-ParentSpanId", "a");
    accessor.setNativeHeader("X-B3-SpanId", "a");

    channel.send(MessageBuilder.withPayload("foo")
        .copyHeaders(accessor.toMessageHeaders())
        .build());

    assertThat(messages)
        .hasSize(1)
        .flatExtracting(s -> ((Map) s.getHeaders().get(NATIVE_HEADERS)).keySet())
        .doesNotContain("X-B3-ParentSpanId");
    assertThat(messages)
        .hasSize(1)
        .flatExtracting(s -> ((Map) s.getHeaders().get(NATIVE_HEADERS)).values())
        .doesNotContain(Collections.singletonList("a"));
  }

  /**
   * The handler is written to be global, meaning it does both producer and consumer side. This
   * masks so we can test only the producer side of tracing.
   */
  ChannelInterceptor producerSideOnly(ChannelInterceptor interceptor) {
    return new ChannelInterceptorAdapter() {
      @Override public Message<?> preSend(Message<?> message, MessageChannel channel) {
        return interceptor.preSend(message, channel);
      }

      @Override
      public void afterSendCompletion(Message<?> message, MessageChannel channel, boolean sent,
          Exception ex) {
        interceptor.afterSendCompletion(message, channel, sent, ex);
      }
    };
  }

  @After public void close() {
    Tracing.current().close();
  }
}
