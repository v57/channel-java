package v57.channel;

import java.util.Iterator;

public final class ChannelStreams {
  private ChannelStreams() {
  }

  public static <T> ChannelStream<T> fromIterable(Iterable<T> iterable) {
    return fromIterator(iterable.iterator());
  }

  public static <T> ChannelStream<T> fromIterator(Iterator<T> iterator) {
    return new ChannelStream<>() {
      @Override
      public StreamNext<T> next() {
        if (iterator.hasNext()) {
          return StreamNext.item(iterator.next());
        }
        return StreamNext.done();
      }
    };
  }

  public static <T> ChannelStream<T> fromValues(Values<T> values) {
    return new ChannelStream<>() {
      @Override
      public StreamNext<T> next() {
        return values.next();
      }

      @Override
      public void close() {
        values.close();
      }
    };
  }
}
