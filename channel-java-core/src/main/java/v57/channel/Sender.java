package v57.channel;

import java.util.function.Consumer;

public final class Sender<State> {
  private final Peer<State> peer;

  Sender(Peer<State> peer) {
    this.peer = peer;
  }

  public CallResult<Object> send(String path) {
    return send(JsonCodec.DYNAMIC, path, null, null);
  }

  public CallResult<Object> send(String path, Object body) {
    return send(JsonCodec.DYNAMIC, path, body, null);
  }

  public <Resp> CallResult<Resp> send(Class<Resp> responseType, String path) {
    return send(TypeRef.of(responseType), path, null, null);
  }

  public <Resp> CallResult<Resp> send(Class<Resp> responseType, String path, Object body) {
    return send(TypeRef.of(responseType), path, body, null);
  }

  public <Resp> CallResult<Resp> send(TypeRef<Resp> responseType, String path, Object body, Object context) {
    return send(JsonCodec.DYNAMIC, responseType, JsonCodec.DYNAMIC, path, body, context);
  }

  public <Req, Resp, Ctx> CallResult<Resp> send(
      TypeRef<Req> requestType,
      TypeRef<Resp> responseType,
      TypeRef<Ctx> contextType,
      String path,
      Req body,
      Ctx context
  ) {
    return peer.sendBlocking(requestType, responseType, contextType, path, body, context);
  }

  public <Item> Values<Item> values(Class<Item> itemType, String path) {
    return values(TypeRef.of(itemType), path, null, null);
  }

  public <Item> Values<Item> values(Class<Item> itemType, String path, Object body) {
    return values(TypeRef.of(itemType), path, body, null);
  }

  public <Item> Values<Item> values(TypeRef<Item> itemType, String path, Object body, Object context) {
    return values(JsonCodec.DYNAMIC, itemType, JsonCodec.DYNAMIC, path, body, context);
  }

  public <Req, Item, Ctx> Values<Item> values(
      TypeRef<Req> requestType,
      TypeRef<Item> itemType,
      TypeRef<Ctx> contextType,
      String path,
      Req body,
      Ctx context
  ) {
    return peer.openValues(requestType, itemType, contextType, path, body, context);
  }

  public <Event> SubscribeResult subscribe(Class<Event> eventType, String path, Object body, Consumer<Event> event) {
    return subscribe(TypeRef.of(eventType), path, body, null, event);
  }

  public <Event> SubscribeResult subscribe(TypeRef<Event> eventType, String path, Object body, Object context, Consumer<Event> event) {
    return subscribe(JsonCodec.DYNAMIC, eventType, JsonCodec.DYNAMIC, path, body, context, event);
  }

  public <Req, Event, Ctx> SubscribeResult subscribe(
      TypeRef<Req> requestType,
      TypeRef<Event> eventType,
      TypeRef<Ctx> contextType,
      String path,
      Req body,
      Ctx context,
      Consumer<Event> event
  ) {
    return peer.subscribeBlocking(requestType, eventType, contextType, path, body, context, event);
  }

  public boolean isRemoteSubscribed(String topic) {
    return peer.isRemoteSubscribed(topic);
  }

  public boolean isConnected() {
    return peer.isConnected();
  }

  public void stop() {
    peer.stop();
  }
}
