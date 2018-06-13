package brave.grpc;

import brave.propagation.TraceContext;
import io.grpc.Metadata.BinaryMarshaller;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.logging.Level.FINE;

/**
 * This logs instead of throwing exceptions.
 *
 * <p>See https://github.com/census-instrumentation/opencensus-specs/blob/master/encodings/BinaryEncoding.md
 */
final class TraceContextBinaryMarshaller implements BinaryMarshaller<TraceContext> {
  static final Logger logger = Logger.getLogger(TraceContextBinaryMarshaller.class.getName());
  static final byte
      VERSION = 0,
      TRACE_ID_FIELD_ID = 0,
      SPAN_ID_FIELD_ID = 1,
      TRACE_OPTION_FIELD_ID = 2;

  private static final int FORMAT_LENGTH =
      4 /* version + 3 fields */ + 16 /* trace ID */ + 8 /* span ID */ + 1 /* sampled bit */;

  @Override public byte[] toBytes(TraceContext traceContext) {
    checkNotNull(traceContext, "traceContext");
    byte[] bytes = new byte[FORMAT_LENGTH];
    bytes[0] = VERSION;
    bytes[1] = TRACE_ID_FIELD_ID;
    writeLong(bytes, 2, traceContext.traceIdHigh());
    writeLong(bytes, 10, traceContext.traceId());
    bytes[18] = SPAN_ID_FIELD_ID;
    writeLong(bytes, 19, traceContext.spanId());
    bytes[27] = TRACE_OPTION_FIELD_ID;
    if (traceContext.sampled() != null && traceContext.sampled()) {
      bytes[28] = 1;
    }
    return bytes;
  }

  @Override public TraceContext parseBytes(byte[] bytes) {
    if (bytes == null) throw new NullPointerException("bytes == null"); // programming error
    if (bytes.length == 0) return null;
    if (bytes[0] != VERSION) {
      logger.log(FINE, "Invalid input: unsupported version {0}", bytes[0]);
      return null;
    }
    if (bytes.length < FORMAT_LENGTH - 2 /* sampled field + bit is optional */) {
      logger.fine("Invalid input: truncated");
      return null;
    }
    long traceIdHigh, traceId, spanId;
    Boolean sampled = null;
    int pos = 1;
    if (bytes[pos] == TRACE_ID_FIELD_ID) {
      pos++;
      traceIdHigh = readLong(bytes, pos);
      traceId = readLong(bytes, pos + 8);
      pos += 16;
    } else {
      logger.log(FINE, "Invalid input: expected trace ID at offset {0}", pos);
      return null;
    }
    if (bytes[pos] == SPAN_ID_FIELD_ID) {
      pos++;
      spanId = readLong(bytes, pos);
      pos += 8;
    } else {
      logger.log(FINE, "Invalid input: expected span ID at offset {0}", pos);
      return null;
    }
    // The trace options field is optional. However, when present, it should be valid.
    if (bytes.length > pos && bytes[pos] == TRACE_OPTION_FIELD_ID) {
      pos++;
      if (bytes.length < pos + 1) {
        logger.log(FINE, "Invalid input: truncated");
        return null;
      }
      sampled = bytes[pos] == 1;
    }
    return TraceContext.newBuilder()
        .traceIdHigh(traceIdHigh)
        .traceId(traceId)
        .spanId(spanId)
        .sampled(sampled)
        .build();
  }

  /** Inspired by {@code okio.Buffer.writeLong} */
  static void writeLong(byte[] data, int pos, long v) {
    data[pos + 0] = (byte) ((v >>> 56L) & 0xff);
    data[pos + 1] = (byte) ((v >>> 48L) & 0xff);
    data[pos + 2] = (byte) ((v >>> 40L) & 0xff);
    data[pos + 3] = (byte) ((v >>> 32L) & 0xff);
    data[pos + 4] = (byte) ((v >>> 24L) & 0xff);
    data[pos + 5] = (byte) ((v >>> 16L) & 0xff);
    data[pos + 6] = (byte) ((v >>> 8L) & 0xff);
    data[pos + 7] = (byte) (v & 0xff);
  }

  /** Inspired by {@code okio.Buffer.readLong} */
  static long readLong(byte[] data, int pos) {
    return (data[pos] & 0xffL) << 56
        | (data[pos + 1] & 0xffL) << 48
        | (data[pos + 2] & 0xffL) << 40
        | (data[pos + 3] & 0xffL) << 32
        | (data[pos + 4] & 0xffL) << 24
        | (data[pos + 5] & 0xffL) << 16
        | (data[pos + 6] & 0xffL) << 8
        | (data[pos + 7] & 0xffL);
  }
}
