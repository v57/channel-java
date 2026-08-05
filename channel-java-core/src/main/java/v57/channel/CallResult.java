package v57.channel;

public final class CallResult<T> {
  private final T value;
  private final String remoteErr;

  private CallResult(T value, String remoteErr) {
    this.value = value;
    this.remoteErr = remoteErr;
  }

  public static <T> CallResult<T> ok(T value) {
    return new CallResult<>(value, null);
  }

  public static <T> CallResult<T> remoteErr(String remoteErr) {
    return new CallResult<>(null, remoteErr);
  }

  public boolean isOk() {
    return remoteErr == null;
  }

  public T value() {
    return value;
  }

  public String remoteErr() {
    return remoteErr;
  }

  public T requireValue() {
    if (remoteErr != null) {
      throw new ChannelException(remoteErr);
    }
    return value;
  }
}
