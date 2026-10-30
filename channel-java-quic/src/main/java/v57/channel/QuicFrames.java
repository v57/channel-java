package v57.channel;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Channel over QUIC (the TypeScript reference's {@code channel/quic}, {@code docs/quic.md} there): one bidirectional
 * stream per connection carrying the same JSON messages as a WebSocket, each one prefixed with its length as a QUIC
 * variable-length integer (RFC 9000 section 16). An empty frame is a keepalive.
 */
final class QuicFrames {
  /** The QUIC application protocol of a Channel connection */
  static final String ALPN = "channel/1";
  /** Silent connections close after this; clients send a keepalive every {@link #KEEPALIVE_SECONDS} */
  static final int IDLE_TIMEOUT_SECONDS = 30;
  static final int KEEPALIVE_SECONDS = 15;
  static final byte[] KEEPALIVE = {0};

  private QuicFrames() {
  }

  static byte[] encode(String text) {
    return encode(text.getBytes(StandardCharsets.UTF_8));
  }

  /** The varint length of {@code payload} followed by {@code payload} */
  static byte[] encode(byte[] payload) {
    long n = payload.length;
    var size = n < (1 << 6) ? 1 : n < (1 << 14) ? 2 : n < (1L << 30) ? 4 : 8;
    var frame = new byte[size + payload.length];
    for (var i = size - 1; i >= 0; i -= 1) {
      frame[i] = (byte) n;
      n >>>= 8;
    }
    frame[0] |= (byte) ((31 - Integer.numberOfLeadingZeros(size)) << 6);
    System.arraycopy(payload, 0, frame, size, payload.length);
    return frame;
  }

  /** A frame that declares more than the allowed payload; the connection is closed before reading it. */
  static final class FrameTooLargeException extends IOException {
    FrameTooLargeException(long length) {
      super("quic frame too large: " + length + " bytes");
    }
  }

  /** Splits a stream back into message payloads, skipping keepalives. */
  static final class Reader {
    private final InputStream input;
    private final int maxPayload;

    Reader(InputStream input, int maxPayload) {
      this.input = input;
      this.maxPayload = maxPayload;
    }

    /** The next non-empty payload, or null when the stream ends between frames. */
    byte[] next() throws IOException {
      while (true) {
        var first = input.read();
        if (first < 0) {
          return null;
        }
        var size = 1 << (first >> 6);
        long length = first & 0x3f;
        for (var i = 1; i < size; i += 1) {
          length = (length << 8) | readByte();
        }
        if (length > maxPayload) {
          throw new FrameTooLargeException(length);
        }
        if (length == 0) {
          continue;
        }
        var payload = input.readNBytes((int) length);
        if (payload.length < length) {
          throw new EOFException("quic stream ended inside a frame");
        }
        return payload;
      }
    }

    private int readByte() throws IOException {
      var next = input.read();
      if (next < 0) {
        throw new EOFException("quic stream ended inside a frame");
      }
      return next;
    }
  }
}
