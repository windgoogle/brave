package brave.spring.messaging;

import brave.propagation.Propagation;
import brave.propagation.PropagationSetterTest;
import java.util.Collections;
import org.springframework.messaging.support.MessageHeaderAccessor;

import static brave.spring.messaging.TracingChannelInterceptor.SETTER;

public class MessageHeaderAccessorSetterTest
    extends PropagationSetterTest<MessageHeaderAccessor, String> {
  MessageHeaderAccessor carrier = new MessageHeaderAccessor();

  @Override public Propagation.KeyFactory<String> keyFactory() {
    return Propagation.KeyFactory.STRING;
  }

  @Override protected MessageHeaderAccessor carrier() {
    return carrier;
  }

  @Override protected Propagation.Setter<MessageHeaderAccessor, String> setter() {
    return SETTER;
  }

  @Override protected Iterable<String> read(MessageHeaderAccessor carrier, String key) {
    Object result = carrier.getHeader(key);
    return result != null ? Collections.singleton(result.toString()) : Collections.emptyList();
  }
}
