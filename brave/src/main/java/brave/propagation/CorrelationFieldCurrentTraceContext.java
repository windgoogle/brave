package brave.propagation;

import brave.internal.HexCodec;
import brave.internal.Nullable;

import static brave.internal.HexCodec.lowerHexEqualsTraceId;
import static brave.internal.HexCodec.lowerHexEqualsUnsignedLong;

/**
 * Adds correlation properties "traceId", "parentId" and "spanId" when a {@link brave.Tracer#currentSpan()
 * span is current}.
 */
public abstract class CorrelationFieldCurrentTraceContext extends CurrentTraceContext {

  final CurrentTraceContext delegate;

  protected CorrelationFieldCurrentTraceContext(CurrentTraceContext delegate) {
    if (delegate == null) throw new NullPointerException("delegate == null");
    this.delegate = delegate;
  }

  @Override public final TraceContext get() {
    return delegate.get();
  }

  @Override public final Scope newScope(@Nullable TraceContext currentSpan) {
    return newScope(currentSpan, getIfString("traceId"), getIfString("spanId"));
  }

  @Override public final Scope maybeScope(@Nullable TraceContext currentSpan) {
    String previousTraceId = getIfString("traceId");
    String previousSpanId = getIfString("spanId");
    if (currentSpan == null) {
      if (previousTraceId == null) return Scope.NOOP;
      return newScope(null, previousTraceId, previousSpanId);
    }
    if (lowerHexEqualsTraceId(previousTraceId, currentSpan)
        && lowerHexEqualsUnsignedLong(previousSpanId, currentSpan.spanId())) {
      return Scope.NOOP;
    }
    return newScope(currentSpan, previousTraceId, previousSpanId);
  }

  // all input parameters are nullable
  Scope newScope(TraceContext currentSpan, String previousTraceId, String previousSpanId) {
    String previousParentId = getIfString("parentId");
    if (currentSpan != null) {
      maybeReplaceTraceContext(currentSpan, previousTraceId, previousParentId, previousSpanId);
    } else {
      remove("traceId");
      remove("parentId");
      remove("spanId");
    }

    Scope scope = delegate.newScope(currentSpan);
    class CorrelationFieldCurrentTraceContextScope implements Scope {
      @Override public void close() {
        scope.close();
        replace("traceId", previousTraceId);
        replace("parentId", previousParentId);
        replace("spanId", previousSpanId);
      }
    }
    return new CorrelationFieldCurrentTraceContextScope();
  }

  void maybeReplaceTraceContext(
      TraceContext currentSpan,
      String previousTraceId,
      @Nullable String previousParentId,
      String previousSpanId
  ) {
    boolean sameTraceId = lowerHexEqualsTraceId(previousTraceId, currentSpan);
    if (!sameTraceId) put("traceId", currentSpan.traceIdString());

    long parentId = currentSpan.parentIdAsLong();
    if (parentId == 0L) {
      remove("parentId");
    } else {
      boolean sameParentId = lowerHexEqualsUnsignedLong(previousParentId, parentId);
      if (!sameParentId) put("parentId", HexCodec.toLowerHex(parentId));
    }

    boolean sameSpanId = lowerHexEqualsUnsignedLong(previousSpanId, currentSpan.spanId());
    if (!sameSpanId) put("spanId", HexCodec.toLowerHex(currentSpan.spanId()));
  }

  protected abstract @Nullable String getIfString(String key);

  protected abstract void put(String key, String value);

  protected abstract void remove(String key);

  final void replace(String key, @Nullable String value) {
    if (value != null) {
      put(key, value);
    } else {
      remove(key);
    }
  }
}
