package v57.channel;

public final class ChannelException extends RuntimeException {
  public ChannelException(String message) {
    super(message);
  }

  public ChannelException(String message, Throwable cause) {
    super(message, cause);
  }
}
