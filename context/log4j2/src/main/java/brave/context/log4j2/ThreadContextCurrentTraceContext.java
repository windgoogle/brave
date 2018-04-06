package brave.context.log4j2;

import brave.propagation.CorrelationFieldCurrentTraceContext;
import brave.propagation.CurrentTraceContext;
import org.apache.logging.log4j.ThreadContext;

/**
 * Adds {@linkplain ThreadContext} properties "traceId", "parentId" and "spanId" when a {@link
 * brave.Tracer#currentSpan() span is current}. These can be used in log correlation.
 */
public final class ThreadContextCurrentTraceContext extends CorrelationFieldCurrentTraceContext {
  public static ThreadContextCurrentTraceContext create() {
    return create(CurrentTraceContext.Default.inheritable());
  }

  public static ThreadContextCurrentTraceContext create(CurrentTraceContext delegate) {
    return new ThreadContextCurrentTraceContext(delegate);
  }

  ThreadContextCurrentTraceContext(CurrentTraceContext delegate) {
    super(delegate);
  }

  @Override protected String getIfString(String key) {
    return ThreadContext.get(key);
  }

  @Override protected void put(String key, String value) {
    ThreadContext.put(key, value);
  }

  @Override protected void remove(String key) {
    ThreadContext.remove(key);
  }
}
