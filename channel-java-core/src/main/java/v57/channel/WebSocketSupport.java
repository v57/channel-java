package v57.channel;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

final class WebSocketSupport {
  private static final String ACCEPT_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
  private static final SecureRandom RANDOM = new SecureRandom();

  private WebSocketSupport() {
  }

  static String clientKey() {
    var nonce = new byte[16];
    RANDOM.nextBytes(nonce);
    return Base64.getEncoder().encodeToString(nonce);
  }

  static String acceptFor(String clientKey) {
    try {
      var digest = MessageDigest.getInstance("SHA-1");
      var bytes = digest.digest((clientKey + ACCEPT_GUID).getBytes(StandardCharsets.US_ASCII));
      return Base64.getEncoder().encodeToString(bytes);
    } catch (Exception error) {
      throw new ChannelException("failed to compute websocket accept", error);
    }
  }

  static ByteBuffer textFrame(String payload, boolean masked) {
    return frame(0x1, payload == null ? new byte[0] : payload.getBytes(StandardCharsets.UTF_8), masked);
  }

  static ByteBuffer pongFrame(byte[] payload, boolean masked) {
    return frame(0xA, payload == null ? new byte[0] : payload, masked);
  }

  static ByteBuffer closeFrame(boolean masked) {
    return frame(0x8, new byte[0], masked);
  }

  static ByteBuffer frame(int opcode, byte[] payload, boolean masked) {
    var safePayload = payload == null ? new byte[0] : payload;
    var payloadLength = safePayload.length;
    var headerLength = 2;
    if (payloadLength > 125 && payloadLength <= 0xFFFF) {
      headerLength += 2;
    } else if (payloadLength > 0xFFFF) {
      headerLength += 8;
    }
    if (masked) {
      headerLength += 4;
    }

    var buffer = ByteBuffer.allocate(headerLength + payloadLength);
    buffer.put((byte) (0x80 | (opcode & 0x0F)));

    if (payloadLength <= 125) {
      buffer.put((byte) ((masked ? 0x80 : 0) | payloadLength));
    } else if (payloadLength <= 0xFFFF) {
      buffer.put((byte) ((masked ? 0x80 : 0) | 126));
      buffer.putShort((short) payloadLength);
    } else {
      buffer.put((byte) ((masked ? 0x80 : 0) | 127));
      buffer.putLong(payloadLength);
    }

    if (masked) {
      var mask = new byte[4];
      RANDOM.nextBytes(mask);
      buffer.put(mask);
      for (var i = 0; i < safePayload.length; i += 1) {
        buffer.put((byte) (safePayload[i] ^ mask[i % 4]));
      }
    } else {
      buffer.put(safePayload);
    }

    buffer.flip();
    return buffer;
  }

  /** Default cap for one websocket message (frame or reassembled fragments). Matches Bun's 16 MiB default. */
  static final int DEFAULT_MAX_PAYLOAD = 16 * 1024 * 1024;
  /** Cap for the HTTP upgrade request/response head. */
  static final int MAX_HANDSHAKE_BYTES = 64 * 1024;

  static Frame decode(byte[] bytes, int length) {
    return decode(bytes, length, DEFAULT_MAX_PAYLOAD);
  }

  /**
   * Decodes one frame from the head of {@code bytes}. Returns null when more bytes are needed.
   * Throws {@link ChannelException} for frames that declare a payload above {@code maxPayload}
   * (including 64-bit lengths with the top bit set), so the caller can drop the connection before
   * buffering the payload.
   */
  static Frame decode(byte[] bytes, int length, int maxPayload) {
    if (length < 2) {
      return null;
    }

    var first = bytes[0] & 0xFF;
    var second = bytes[1] & 0xFF;
    var fin = (first & 0x80) != 0;
    var opcode = first & 0x0F;
    var masked = (second & 0x80) != 0;
    long payloadLength = second & 0x7F;
    var cursor = 2;

    if (payloadLength == 126) {
      if (length < cursor + 2) {
        return null;
      }
      payloadLength = ((bytes[cursor] & 0xFF) << 8) | (bytes[cursor + 1] & 0xFF);
      cursor += 2;
    } else if (payloadLength == 127) {
      if (length < cursor + 8) {
        return null;
      }
      payloadLength = 0;
      for (var i = 0; i < 8; i += 1) {
        payloadLength = (payloadLength << 8) | (bytes[cursor + i] & 0xFFL);
      }
      cursor += 8;
    }
    // A 64-bit length with the top bit set wraps negative: reject it together with oversized ones.
    if (payloadLength < 0 || payloadLength > maxPayload) {
      throw new ChannelException("websocket frame too large");
    }

    byte[] mask = null;
    if (masked) {
      if (length < cursor + 4) {
        return null;
      }
      mask = new byte[]{bytes[cursor], bytes[cursor + 1], bytes[cursor + 2], bytes[cursor + 3]};
      cursor += 4;
    }

    if ((long) length < cursor + payloadLength) {
      return null;
    }

    var payload = new byte[(int) payloadLength];
    System.arraycopy(bytes, cursor, payload, 0, payload.length);
    if (mask != null) {
      for (var i = 0; i < payload.length; i += 1) {
        payload[i] = (byte) (payload[i] ^ mask[i % 4]);
      }
    }

    return new Frame(fin, opcode, payload, cursor + payload.length);
  }

  /**
   * Reassembles fragmented messages (RFC 6455 section 5.4): a data frame with FIN=0 followed by
   * continuation frames (opcode 0), with control frames allowed in between. Returns complete
   * messages and control frames; returns null while a message is still incomplete.
   */
  static final class MessageAssembler {
    private final int maxPayload;
    private int opcode = -1;
    private byte[] buffer = new byte[0];
    private int size;

    MessageAssembler(int maxPayload) {
      this.maxPayload = maxPayload;
    }

    Frame accept(Frame frame) {
      var op = frame.opcode();
      if (op >= 0x8) {
        if (!frame.fin() || frame.payload().length > 125) {
          throw new ChannelException("invalid websocket control frame");
        }
        return frame;
      }
      if (op == 0x0) {
        if (opcode < 0) {
          throw new ChannelException("unexpected websocket continuation frame");
        }
        append(frame.payload());
        if (!frame.fin()) {
          return null;
        }
        var message = new Frame(true, opcode, java.util.Arrays.copyOf(buffer, size), frame.consumed());
        opcode = -1;
        buffer = new byte[0];
        size = 0;
        return message;
      }
      if (opcode >= 0) {
        throw new ChannelException("new websocket message before the previous one finished");
      }
      if (frame.fin()) {
        return frame;
      }
      opcode = op;
      size = 0;
      append(frame.payload());
      return null;
    }

    private void append(byte[] data) {
      if ((long) size + data.length > maxPayload) {
        throw new ChannelException("websocket message too large");
      }
      if (size + data.length > buffer.length) {
        buffer = java.util.Arrays.copyOf(buffer, Math.max(size + data.length, buffer.length * 2));
      }
      System.arraycopy(data, 0, buffer, size, data.length);
      size += data.length;
    }
  }

  record Frame(boolean fin, int opcode, byte[] payload, int consumed) {
    String text() {
      return new String(payload, StandardCharsets.UTF_8);
    }
  }
}
