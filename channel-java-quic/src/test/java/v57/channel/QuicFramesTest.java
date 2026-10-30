package v57.channel;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class QuicFramesTest {
  @Test
  void lengthsUseTheShortestVarintAndKeepalivesAreSkipped() throws Exception {
    for (var size : new int[] {0, 1, 63, 64, 16383, 16384, 70000}) {
      var payload = new byte[size];
      Arrays.fill(payload, (byte) 'x');
      var frame = QuicFrames.encode(payload);
      var header = frame.length - size;
      assertEquals(size < 64 ? 1 : size < 16384 ? 2 : 4, header, "header bytes for " + size);

      // A keepalive in front is skipped
      var bytes = new ByteArrayOutputStream();
      bytes.write(0);
      bytes.write(frame);
      bytes.write(QuicFrames.encode("next"));
      var reader = new QuicFrames.Reader(new ByteArrayInputStream(bytes.toByteArray()), QuicFrames.KEEPALIVE.length + 100_000);
      if (size > 0) {
        assertArrayEquals(payload, reader.next(), "payload of " + size);
      }
      assertEquals("next", new String(reader.next(), StandardCharsets.UTF_8));
      assertNull(reader.next());
    }
  }

  @Test
  void eightByteLengthsDecode() throws Exception {
    var big = new byte[] {(byte) 0xc0, 0, 0, 0, 0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o'};
    assertEquals("hello", new String(new QuicFrames.Reader(new ByteArrayInputStream(big), 100).next(), StandardCharsets.UTF_8));
  }

  @Test
  void framesOverTheLimitAreRefusedBeforeTheirPayload() {
    var frame = QuicFrames.encode(new byte[11]);
    assertThrows(QuicFrames.FrameTooLargeException.class, () -> new QuicFrames.Reader(new ByteArrayInputStream(frame), 10).next());
    // Only the header is there: the reader doesn't wait for 16 MiB + 1 of payload
    var header = new byte[] {(byte) 0x81, 0, 0, 1};
    assertThrows(QuicFrames.FrameTooLargeException.class,
        () -> new QuicFrames.Reader(new ByteArrayInputStream(header), 16 * 1024 * 1024).next());
  }

  @Test
  void aStreamEndingInsideAFrameFails() {
    var frame = Arrays.copyOf(QuicFrames.encode("hello"), 3);
    assertThrows(EOFException.class, () -> new QuicFrames.Reader(new ByteArrayInputStream(frame), 100).next());
    assertThrows(EOFException.class, () -> new QuicFrames.Reader(new ByteArrayInputStream(new byte[] {0x40}), 100).next());
  }

  @Test
  void fingerprintsAndCertificateNames() throws Exception {
    assertEquals(32, QuicTls.fingerprint("DD:D2:37:00:BD:8A:BD:29:14:15:D8:65:84:BE:C2:80:10:FF:21:DA:A1:C5:9D:B3:4A:7F:36:63:EF:4B:55:9B").length);
    assertThrows(ChannelException.class, () -> QuicTls.fingerprint("nope"));
    assertThrows(ChannelException.class, () -> QuicTls.fingerprint("zz".repeat(32)));
    assertTrue(QuicTls.isIpLiteral("127.0.0.1"));
    assertTrue(QuicTls.isIpLiteral("::1"));
    assertFalse(QuicTls.isIpLiteral("localhost"));
  }
}
