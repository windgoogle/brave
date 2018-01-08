package brave.spring.messaging;

import brave.Tracing;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.ChannelInterceptorAdapter;
import org.springframework.messaging.support.ExecutorSubscribableChannel;
import org.springframework.messaging.support.MessageBuilder;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;

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

    assertThat(messages).hasSize(1);
    assertThat(spans).hasSize(1);
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
