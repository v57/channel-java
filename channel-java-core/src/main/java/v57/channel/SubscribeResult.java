package v57.channel;

public final class SubscribeResult {
  private final SubscriptionHandle handle;
  private final String remoteErr;

  private SubscribeResult(SubscriptionHandle handle, String remoteErr) {
    this.handle = handle;
    this.remoteErr = remoteErr;
  }

  public static SubscribeResult ok(SubscriptionHandle handle) {
    return new SubscribeResult(handle, null);
  }

  public static SubscribeResult remoteErr(String remoteErr) {
    return new SubscribeResult(null, remoteErr);
  }

  public boolean isOk() {
    return remoteErr == null;
  }

  public SubscriptionHandle handle() {
    return handle;
  }

  public String remoteErr() {
    return remoteErr;
  }
}
