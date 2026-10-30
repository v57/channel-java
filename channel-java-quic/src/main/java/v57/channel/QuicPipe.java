package v57.channel;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import tech.kwik.core.QuicStream;

/**
 * The framed stream of one connection. kwik's streams block, so a reader thread and a writer thread move frames
 * between the stream and two queues, and the pumping thread (poll) only touches the queues: the {@link Peer} stays
 * single-threaded like with the WebSocket transports.
 */
final class QuicPipe {
  /** Messages read but not handled yet; a full inbox stops reading, so QUIC flow control slows the peer down. */
  private static final int INBOX_CAPACITY = 1024;

  private final int maxPayload;
  private final boolean keepalive;
  private final Runnable onEnd;
  private final LinkedBlockingQueue<byte[]> outbound = new LinkedBlockingQueue<>();
  private final ArrayBlockingQueue<String> inbox = new ArrayBlockingQueue<>(INBOX_CAPACITY);
  private final AtomicBoolean ended = new AtomicBoolean();
  private volatile boolean closed;
  private Thread reader;
  private Thread writer;

  /**
   * @param keepalive send an empty frame when nothing was written for 15 s (the client side)
   * @param onEnd     runs once, on a pipe thread, when the stream ends or fails
   */
  QuicPipe(int maxPayload, boolean keepalive, Runnable onEnd) {
    this.maxPayload = maxPayload;
    this.keepalive = keepalive;
    this.onEnd = onEnd;
  }

  /** Queues one message; frames queued before {@link #start} go out first, in order. */
  void send(String payload) {
    if (!closed) {
      outbound.add(QuicFrames.encode(payload));
    }
  }

  synchronized void start(QuicStream stream, String name) {
    if (closed) {
      return;
    }
    reader = new Thread(() -> read(stream), name + "-read");
    writer = new Thread(() -> write(stream), name + "-write");
    reader.setDaemon(true);
    writer.setDaemon(true);
    reader.start();
    writer.start();
  }

  /** The next message read from the stream, or null. */
  String poll() {
    return inbox.poll();
  }

  /** The stream ended or failed (messages read before that may still wait in the inbox). */
  boolean ended() {
    return ended.get();
  }

  boolean hasInbox() {
    return !inbox.isEmpty();
  }

  synchronized void close() {
    closed = true;
    outbound.clear();
    ended.set(true);
    if (reader != null) {
      reader.interrupt();
    }
    if (writer != null) {
      writer.interrupt();
    }
  }

  private void read(QuicStream stream) {
    try {
      var frames = new QuicFrames.Reader(new BufferedInputStream(stream.getInputStream(), 64 * 1024), maxPayload);
      while (!closed) {
        var payload = frames.next();
        if (payload == null) {
          break;
        }
        inbox.put(new String(payload, StandardCharsets.UTF_8));
      }
    } catch (IOException | InterruptedException | RuntimeException ignored) {
      // The peer went away, sent a frame over the limit, or the connection was closed here
    }
    end();
  }

  private void write(QuicStream stream) {
    try {
      var output = stream.getOutputStream();
      while (!closed) {
        var frame = keepalive ? outbound.poll(QuicFrames.KEEPALIVE_SECONDS, TimeUnit.SECONDS) : outbound.take();
        output.write(frame == null ? QuicFrames.KEEPALIVE : frame);
        // Frames queued meanwhile go out in the same flush
        for (var more = outbound.poll(); more != null && !closed; more = outbound.poll()) {
          output.write(more);
        }
        output.flush();
      }
    } catch (IOException | InterruptedException | RuntimeException ignored) {
      // Closed here or by the peer
    }
    end();
  }

  private void end() {
    // A local close() already set ended: the closer handles the connection itself
    if (ended.compareAndSet(false, true)) {
      closed = true;
      if (onEnd != null) {
        try {
          onEnd.run();
        } catch (RuntimeException ignored) {
        }
      }
    }
  }
}
