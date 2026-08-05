package v57.channel;

public final class StreamNext<T> {
  public enum Kind {
    ITEM,
    DONE,
    REMOTE_ERR
  }

  private final Kind kind;
  private final T item;
  private final String remoteErr;

  private StreamNext(Kind kind, T item, String remoteErr) {
    this.kind = kind;
    this.item = item;
    this.remoteErr = remoteErr;
  }

  public static <T> StreamNext<T> item(T item) {
    return new StreamNext<>(Kind.ITEM, item, null);
  }

  public static <T> StreamNext<T> done() {
    return new StreamNext<>(Kind.DONE, null, null);
  }

  public static <T> StreamNext<T> remoteErr(String remoteErr) {
    return new StreamNext<>(Kind.REMOTE_ERR, null, remoteErr);
  }

  public Kind kind() {
    return kind;
  }

  public T item() {
    return item;
  }

  public String remoteErr() {
    return remoteErr;
  }

  public boolean isItem() {
    return kind == Kind.ITEM;
  }

  public boolean isDone() {
    return kind == Kind.DONE;
  }

  public boolean isRemoteErr() {
    return kind == Kind.REMOTE_ERR;
  }
}
