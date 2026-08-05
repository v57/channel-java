package v57.channel;

public final class Values<T> implements AutoCloseable {
  private final PendingStream pending;
  private final TypeRef<T> itemType;

  Values(PendingStream pending, TypeRef<T> itemType) {
    this.pending = pending;
    this.itemType = itemType;
  }

  public StreamNext<T> next() {
    return pending.next(itemType);
  }

  @Override
  public void close() {
    pending.close();
  }
}
