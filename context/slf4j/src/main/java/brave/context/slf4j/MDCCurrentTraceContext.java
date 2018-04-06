package brave.context.slf4j;

import brave.propagation.CorrelationFieldCurrentTraceContext;
import brave.propagation.CurrentTraceContext;
import org.slf4j.MDC;

/**
 * Adds {@linkplain MDC} properties "traceId", "parentId" and "spanId" when a {@link
 * brave.Tracer#currentSpan() span is current}. These can be used in log correlation.
 */
public final class MDCCurrentTraceContext extends CorrelationFieldCurrentTraceContext {
  public static MDCCurrentTraceContext create() {
    return create(CurrentTraceContext.Default.inheritable());
  }

  public static MDCCurrentTraceContext create(CurrentTraceContext delegate) {
    return new MDCCurrentTraceContext(delegate);
  }

  MDCCurrentTraceContext(CurrentTraceContext delegate) {
    super(delegate);
  }

  @Override protected String getIfString(String key) {
    return MDC.get(key);
  }

  @Override protected void put(String key, String value) {
    MDC.put(key, value);
  }

  @Override protected void remove(String key) {
    MDC.remove(key);
  }
}
