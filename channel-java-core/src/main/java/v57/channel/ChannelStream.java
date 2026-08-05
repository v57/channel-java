package v57.channel;

public interface ChannelStream<T> extends AutoCloseable {
  StreamNext<T> next();

  @Override
  default void close() {
  }
}
