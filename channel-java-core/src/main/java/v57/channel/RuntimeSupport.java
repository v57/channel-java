package v57.channel;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

interface Pollable {
  boolean poll() throws Exception;
}

interface Transport {
  void queueFrame(String payload);

  void close();

  boolean isConnected();

  default boolean isOpen() {
    return isConnected();
  }

  default boolean isProcess() {
    return false;
  }
}

final class PumpRegistry {
  private static final LinkedHashSet<Pollable> POLLABLES = new LinkedHashSet<>();

  private PumpRegistry() {
  }

  static synchronized void register(Pollable pollable) {
    POLLABLES.add(pollable);
  }

  static synchronized void unregister(Pollable pollable) {
    POLLABLES.remove(pollable);
  }

  static boolean pumpAll() {
    List<Pollable> snapshot;
    synchronized (PumpRegistry.class) {
      snapshot = List.copyOf(POLLABLES);
    }

    var progressed = false;
    for (var pollable : snapshot) {
      try {
        progressed = pollable.poll() || progressed;
      } catch (Exception error) {
        throw new ChannelException("poll failed", error);
      }
    }
    return progressed;
  }
}

final class WireMessage {
  Long id;
  String path;
  String stream;
  String sub;
  String unsub;
  Long cancel;
  String topic;
  String bodyJson;
  String contextJson;
  String error;
  Boolean done;

  static WireMessage request(long id, String path, String bodyJson, String contextJson) {
    var message = new WireMessage();
    message.id = id;
    message.path = path;
    message.bodyJson = bodyJson;
    message.contextJson = contextJson;
    return message;
  }

  static WireMessage stream(long id, String path, String bodyJson, String contextJson) {
    var message = new WireMessage();
    message.id = id;
    message.stream = path;
    message.bodyJson = bodyJson;
    message.contextJson = contextJson;
    return message;
  }

  static WireMessage subscribe(long id, String path, String bodyJson, String contextJson) {
    var message = new WireMessage();
    message.id = id;
    message.sub = path;
    message.bodyJson = bodyJson;
    message.contextJson = contextJson;
    return message;
  }

  static WireMessage response(long id, String bodyJson) {
    var message = new WireMessage();
    message.id = id;
    message.bodyJson = bodyJson;
    return message;
  }

  static WireMessage responseWithTopic(long id, String topic, String bodyJson) {
    var message = response(id, bodyJson);
    message.topic = topic;
    return message;
  }

  static WireMessage event(String topic, String bodyJson) {
    var message = new WireMessage();
    message.topic = topic;
    message.bodyJson = bodyJson;
    return message;
  }

  static WireMessage done(long id) {
    var message = new WireMessage();
    message.id = id;
    message.done = true;
    return message;
  }

  static WireMessage error(long id, String error) {
    var message = new WireMessage();
    message.id = id;
    message.error = error;
    return message;
  }

  static WireMessage cancel(long id) {
    var message = new WireMessage();
    message.cancel = id;
    return message;
  }

  static WireMessage unsubscribe(String topic) {
    var message = new WireMessage();
    message.unsub = topic;
    return message;
  }

  String encode() {
    var builder = new StringBuilder(96);
    appendObject(builder, this);
    return builder.toString();
  }

  static String encodeBatch(List<WireMessage> messages) {
    if (messages.size() == 1) {
      return messages.get(0).encode();
    }
    var builder = new StringBuilder(messages.size() * 64);
    builder.append('[');
    var first = true;
    for (var message : messages) {
      if (!first) {
        builder.append(',');
      }
      first = false;
      appendObject(builder, message);
    }
    builder.append(']');
    return builder.toString();
  }

  @SuppressWarnings("unchecked")
  static List<WireMessage> decodeAll(String payload) {
    var decoded = JsonCodec.decode(payload);
    if (decoded instanceof List<?> list) {
      var result = new ArrayList<WireMessage>(list.size());
      for (var item : list) {
        result.add(fromDynamic(item));
      }
      return result;
    }
    return List.of(fromDynamic(decoded));
  }

  private static WireMessage fromDynamic(Object value) {
    if (!(value instanceof Map<?, ?> map)) {
      throw new ChannelException("invalid json message");
    }
    var message = new WireMessage();
    message.id = asLong(map.get("id"));
    message.path = asString(map.get("path"));
    message.stream = asString(map.get("stream"));
    message.sub = asString(map.get("sub"));
    message.unsub = asString(map.get("unsub"));
    message.cancel = asLong(map.get("cancel"));
    message.topic = asString(map.get("topic"));
    message.error = asString(map.get("error"));
    message.done = asBoolean(map.get("done"));
    if (map.containsKey("body")) {
      message.bodyJson = JsonCodec.encode(map.get("body"));
    }
    if (map.containsKey("context")) {
      message.contextJson = JsonCodec.encode(map.get("context"));
    }
    return message;
  }

  private static void appendObject(StringBuilder builder, WireMessage message) {
    builder.append('{');
    var first = true;
    first = appendRaw(builder, first, "id", message.id == null ? null : Long.toString(message.id));
    first = appendString(builder, first, "path", message.path);
    first = appendString(builder, first, "stream", message.stream);
    first = appendString(builder, first, "sub", message.sub);
    first = appendString(builder, first, "unsub", message.unsub);
    first = appendRaw(builder, first, "cancel", message.cancel == null ? null : Long.toString(message.cancel));
    first = appendString(builder, first, "topic", message.topic);
    first = appendRaw(builder, first, "body", message.bodyJson);
    first = appendRaw(builder, first, "context", message.contextJson);
    first = appendString(builder, first, "error", message.error);
    appendRaw(builder, first, "done", message.done == null ? null : message.done.toString());
    builder.append('}');
  }

  private static boolean appendString(StringBuilder builder, boolean first, String key, String value) {
    if (value == null) {
      return first;
    }
    return appendRaw(builder, first, key, JsonCodec.encode(value));
  }

  private static boolean appendRaw(StringBuilder builder, boolean first, String key, String rawValue) {
    if (rawValue == null) {
      return first;
    }
    if (!first) {
      builder.append(',');
    }
    builder.append(JsonCodec.encode(key)).append(':').append(rawValue);
    return false;
  }

  private static Long asLong(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number.longValue();
    }
    if (value instanceof String text) {
      return Long.parseLong(text);
    }
    return null;
  }

  private static String asString(Object value) {
    if (value == null) {
      return null;
    }
    return String.valueOf(value);
  }

  private static Boolean asBoolean(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Boolean flag) {
      return flag;
    }
    if (value instanceof Number number) {
      return number.longValue() != 0;
    }
    return Boolean.parseBoolean(String.valueOf(value));
  }
}

final class PendingStream {
  private final Peer<?> peer;
  private final long id;
  private final ArrayDeque<String> items = new ArrayDeque<>();
  private boolean done;
  private boolean closed;
  private String error;

  PendingStream(Peer<?> peer, long id) {
    this.peer = peer;
    this.id = id;
  }

  synchronized void accept(WireMessage message) {
    if (message.error != null) {
      error = message.error;
      done = true;
      return;
    }
    if (Boolean.TRUE.equals(message.done)) {
      done = true;
      return;
    }
    items.addLast(message.bodyJson);
  }

  <T> StreamNext<T> next(TypeRef<T> itemType) {
    while (true) {
      String payload;
      synchronized (this) {
        payload = items.pollFirst();
        if (payload != null) {
          return StreamNext.item(JsonCodec.decode(itemType, payload));
        }
        if (error != null) {
          return StreamNext.remoteErr(error);
        }
        if (done) {
          return StreamNext.done();
        }
      }

      if (!peer.isOpen()) {
        return StreamNext.remoteErr("disconnected");
      }
      if (!PumpRegistry.pumpAll()) {
        LockSupport.parkNanos(100_000L);
      }
    }
  }

  void close() {
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      done = true;
      items.clear();
    }
    peer.cancelOutgoingStream(id);
  }
}

final class Peer<State> {
  private final Channel<State> channel;
  private final State state;
  private final Transport transport;
  private final Map<Long, PendingCall> pendingCalls = new LinkedHashMap<>();
  private final Map<Long, PendingStream> pendingStreams = new LinkedHashMap<>();
  private final Map<Long, PendingSubscription<?>> pendingSubscriptions = new LinkedHashMap<>();
  private final Map<Long, LocalActiveStream> localStreams = new LinkedHashMap<>();
  private final Map<String, Map<Integer, LocalTopicListener<?>>> localListeners = new LinkedHashMap<>();
  private final LinkedHashSet<String> remoteTopics = new LinkedHashSet<>();
  private final List<Runnable> publisherDetachers = new ArrayList<>();

  private boolean stopped;
  private boolean disconnectInvoked;
  private boolean pollingLocalStreams;
  private long nextId = 1;
  private int nextListenerId = 1;

  Peer(Channel<State> channel, State state, Transport transport) {
    this.channel = Objects.requireNonNull(channel, "channel");
    this.state = state;
    this.transport = Objects.requireNonNull(transport, "transport");
    attachPublishers();
  }

  State state() {
    return state;
  }

  boolean isConnected() {
    return !stopped && transport.isConnected();
  }

  boolean isOpen() {
    return !stopped && transport.isOpen();
  }

  boolean isRemoteSubscribed(String topic) {
    return remoteTopics.contains(topic);
  }

  <Req, Resp, Ctx> CallResult<Resp> sendBlocking(
      TypeRef<Req> requestType,
      TypeRef<Resp> responseType,
      TypeRef<Ctx> contextType,
      String path,
      Req body,
      Ctx context
  ) {
    if (transport.isProcess()) {
      var direct = channel.tryDirectProcessSend(this, requestType, responseType, contextType, path, body, context);
      if (direct != null) {
        return direct;
      }
    }

    var id = nextId++;
    var pending = new PendingCall();
    pendingCalls.put(id, pending);
    queue(WireMessage.request(
        id,
        path,
        JsonCodec.encodeNullable(requestType, body),
        JsonCodec.encodeNullable(contextType, context)
    ));

    while (!pending.done()) {
      if (!isOpen()) {
        pending.completeError("disconnected");
        break;
      }
      if (!PumpRegistry.pumpAll()) {
        LockSupport.parkNanos(100_000L);
      }
    }
    pendingCalls.remove(id);

    if (pending.error() != null) {
      return CallResult.remoteErr(pending.error());
    }
    return CallResult.ok(JsonCodec.decode(responseType, pending.bodyJson()));
  }

  <Req, Item, Ctx> Values<Item> openValues(
      TypeRef<Req> requestType,
      TypeRef<Item> itemType,
      TypeRef<Ctx> contextType,
      String path,
      Req body,
      Ctx context
  ) {
    var id = nextId++;
    var pending = new PendingStream(this, id);
    pendingStreams.put(id, pending);
    queue(WireMessage.stream(
        id,
        path,
        JsonCodec.encodeNullable(requestType, body),
        JsonCodec.encodeNullable(contextType, context)
    ));
    return new Values<>(pending, itemType);
  }

  <Req, Event, Ctx> SubscribeResult subscribeBlocking(
      TypeRef<Req> requestType,
      TypeRef<Event> eventType,
      TypeRef<Ctx> contextType,
      String path,
      Req body,
      Ctx context,
      Consumer<Event> callback
  ) {
    var id = nextId++;
    var pending = new PendingSubscription<>(eventType, callback);
    pendingSubscriptions.put(id, pending);
    queue(WireMessage.subscribe(
        id,
        path,
        JsonCodec.encodeNullable(requestType, body),
        JsonCodec.encodeNullable(contextType, context)
    ));

    while (!pending.done()) {
      if (!isOpen()) {
        pending.completeError("disconnected");
        break;
      }
      if (!PumpRegistry.pumpAll()) {
        LockSupport.parkNanos(100_000L);
      }
    }
    pendingSubscriptions.remove(id);

    if (pending.error() != null) {
      return SubscribeResult.remoteErr(pending.error());
    }
    if (pending.topic() == null || pending.topic().isEmpty()) {
      return SubscribeResult.remoteErr("missing topic");
    }
    return SubscribeResult.ok(registerLocalListener(pending.topic(), eventType, callback));
  }

  boolean pollLocalStreams() {
    if (pollingLocalStreams) {
      return false;
    }
    pollingLocalStreams = true;
    try {
      var progressed = false;
      var ids = new ArrayList<>(localStreams.keySet());
      for (var id : ids) {
        var stream = localStreams.get(id);
        if (stream == null) {
          continue;
        }
        progressed = advanceLocalStream(id, stream) || progressed;
      }
      return progressed;
    } finally {
      pollingLocalStreams = false;
    }
  }

  void handlePayload(String payload) {
    for (var message : WireMessage.decodeAll(payload)) {
      dispatch(message);
    }
  }

  void stop() {
    if (stopped) {
      return;
    }
    stopped = true;
    transport.close();

    for (var pending : pendingCalls.values()) {
      pending.completeError("disconnected");
    }
    for (var pending : pendingStreams.values()) {
      pending.accept(WireMessage.error(0, "disconnected"));
    }
    for (var pending : pendingSubscriptions.values()) {
      pending.completeError("disconnected");
    }
    pendingCalls.clear();
    pendingStreams.clear();
    pendingSubscriptions.clear();

    for (var stream : localStreams.values()) {
      try {
        stream.stream.close();
      } catch (Throwable ignored) {
      }
    }
    localStreams.clear();
    localListeners.clear();
    remoteTopics.clear();

    if (!disconnectInvoked) {
      disconnectInvoked = true;
      channel.invokeDisconnect(state, new Sender<>(this));
    }

    for (var detach : publisherDetachers) {
      detach.run();
    }
    publisherDetachers.clear();
  }

  void cancelOutgoingStream(long id) {
    pendingStreams.remove(id);
    if (!stopped) {
      queue(WireMessage.cancel(id));
    }
  }

  private boolean advanceLocalStream(long id, LocalActiveStream stream) {
    try {
      var next = stream.stream.next();
      if (next == null) {
        localStreams.remove(id);
        return false;
      }
      if (next.isItem()) {
        queue(WireMessage.response(id, JsonCodec.encode(stream.itemType, next.item())));
        return true;
      }
      if (next.isRemoteErr()) {
        queue(WireMessage.error(id, next.remoteErr()));
        localStreams.remove(id);
        stream.stream.close();
        return true;
      }
      queue(WireMessage.done(id));
      localStreams.remove(id);
      stream.stream.close();
      return true;
    } catch (Throwable error) {
      queue(WireMessage.error(id, RuntimeSupport.errorText(error)));
      localStreams.remove(id);
      try {
        stream.stream.close();
      } catch (Throwable ignored) {
      }
      return true;
    }
  }

  private void dispatch(WireMessage message) {
    if (message.path != null) {
      handlePost(message);
      return;
    }
    if (message.stream != null) {
      handleStream(message);
      return;
    }
    if (message.sub != null) {
      handleSubscribe(message);
      return;
    }
    if (message.unsub != null) {
      remoteTopics.remove(message.unsub);
      return;
    }
    if (message.cancel != null) {
      handleCancel(message.cancel);
      return;
    }
    if (message.id != null) {
      handleResponse(message);
      return;
    }
    if (message.topic != null && message.bodyJson != null) {
      dispatchEvent(message.topic, message.bodyJson);
    }
  }

  private void handlePost(WireMessage message) {
    var binding = channel.lookupPost(message.path);
    if (binding == null) {
      if (message.id != null) {
        queue(WireMessage.error(message.id, "api not found"));
      }
      return;
    }

    try {
      var bodyJson = binding.encodeResponse(binding.invokeFromJson(state, new Sender<>(this), message.path, message.bodyJson, message.contextJson));
      if (message.id != null) {
        queue(WireMessage.response(message.id, bodyJson));
      }
    } catch (Throwable error) {
      if (message.id != null) {
        queue(WireMessage.error(message.id, RuntimeSupport.errorText(error)));
      }
    }
  }

  private void handleStream(WireMessage message) {
    var binding = channel.lookupStream(message.stream);
    if (binding == null) {
      if (message.id != null) {
        queue(WireMessage.error(message.id, "api not found"));
      }
      return;
    }

    try {
      var stream = binding.openFromJson(state, new Sender<>(this), message.stream, message.bodyJson, message.contextJson);
      if (message.id != null) {
        localStreams.put(message.id, new LocalActiveStream(stream, binding.itemType()));
      }
    } catch (Throwable error) {
      if (message.id != null) {
        queue(WireMessage.error(message.id, RuntimeSupport.errorText(error)));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void handleSubscribe(WireMessage message) {
    var subscription = channel.lookupSubscription(message.sub);
    if (subscription == null) {
      if (message.id != null) {
        queue(WireMessage.error(message.id, "subscription not found"));
      }
      return;
    }

    try {
      var typed = (Subscription<Object, Object>) subscription;
      var request = JsonCodec.decode((TypeRef<Object>) typed.requestType(), message.bodyJson);
      var topic = typed.fullTopic(request);
      remoteTopics.add(topic);
      if (message.id != null) {
        queue(WireMessage.responseWithTopic(
            message.id,
            topic,
            JsonCodec.encodeNullable(JsonCodec.DYNAMIC, typed.ackBody(request))
        ));
      }
    } catch (Throwable error) {
      if (message.id != null) {
        queue(WireMessage.error(message.id, RuntimeSupport.errorText(error)));
      }
    }
  }

  private void handleCancel(long id) {
    var stream = localStreams.remove(id);
    if (stream != null) {
      try {
        stream.stream.close();
      } catch (Throwable ignored) {
      }
    }
  }

  private void handleResponse(WireMessage message) {
    var call = pendingCalls.get(message.id);
    if (call != null) {
      if (message.error != null) {
        call.completeError(message.error);
      } else {
        call.completeBody(message.bodyJson);
      }
      pendingCalls.remove(message.id);
      return;
    }

    var stream = pendingStreams.get(message.id);
    if (stream != null) {
      stream.accept(message);
      if (message.error != null || Boolean.TRUE.equals(message.done)) {
        pendingStreams.remove(message.id);
      }
      return;
    }

    var subscription = pendingSubscriptions.get(message.id);
    if (subscription != null) {
      if (message.error != null) {
        subscription.completeError(message.error);
      } else {
        subscription.completeTopic(message.topic);
      }
      return;
    }

    if (message.topic != null && message.bodyJson != null) {
      dispatchEvent(message.topic, message.bodyJson);
    }
  }

  private void dispatchEvent(String topic, String bodyJson) {
    var listeners = localListeners.get(topic);
    if (listeners == null || listeners.isEmpty()) {
      return;
    }

    var snapshot = new ArrayList<>(listeners.values());
    for (var listener : snapshot) {
      listener.dispatch(bodyJson);
    }
  }

  private <Event> SubscriptionHandle registerLocalListener(String topic, TypeRef<Event> eventType, Consumer<Event> callback) {
    var id = nextListenerId++;
    localListeners.computeIfAbsent(topic, ignored -> new LinkedHashMap<>())
        .put(id, new LocalTopicListener<>(eventType, callback));

    return () -> {
      var listeners = localListeners.get(topic);
      if (listeners == null) {
        return;
      }
      listeners.remove(id);
      if (listeners.isEmpty()) {
        localListeners.remove(topic);
        if (!stopped) {
          queue(WireMessage.unsubscribe(topic));
        }
      }
    };
  }

  @SuppressWarnings("unchecked")
  private void attachPublishers() {
    for (var subscription : channel.subscriptions().values()) {
      var typed = (Subscription<Object, Object>) subscription;
      Subscription.Publisher<Object> publisher = event -> {
        if (stopped || !remoteTopics.contains(event.topic())) {
          return;
        }
        queue(WireMessage.event(event.topic(), JsonCodec.encode(typed.eventType(), event.body())));
      };
      typed.addPublisher(publisher);
      publisherDetachers.add(() -> typed.removePublisher(publisher));
    }
  }

  private void queue(WireMessage message) {
    if (stopped) {
      return;
    }
    transport.queueFrame(message.encode());
  }
}

final class PendingCall {
  private boolean done;
  private String bodyJson;
  private String error;

  synchronized void completeBody(String bodyJson) {
    this.bodyJson = bodyJson;
    this.done = true;
  }

  synchronized void completeError(String error) {
    this.error = error;
    this.done = true;
  }

  synchronized boolean done() {
    return done;
  }

  synchronized String bodyJson() {
    return bodyJson;
  }

  synchronized String error() {
    return error;
  }
}

final class PendingSubscription<Event> {
  private final TypeRef<Event> eventType;
  private final Consumer<Event> callback;
  private boolean done;
  private String error;
  private String topic;

  PendingSubscription(TypeRef<Event> eventType, Consumer<Event> callback) {
    this.eventType = eventType;
    this.callback = callback;
  }

  synchronized void completeError(String error) {
    this.error = error;
    this.done = true;
  }

  synchronized void completeTopic(String topic) {
    this.topic = topic;
    this.done = true;
  }

  synchronized boolean done() {
    return done;
  }

  synchronized String error() {
    return error;
  }

  synchronized String topic() {
    return topic;
  }

  TypeRef<Event> eventType() {
    return eventType;
  }

  Consumer<Event> callback() {
    return callback;
  }
}

final class LocalActiveStream {
  final ChannelStream<?> stream;
  final TypeRef<?> itemType;

  LocalActiveStream(ChannelStream<?> stream, TypeRef<?> itemType) {
    this.stream = stream;
    this.itemType = itemType;
  }
}

final class LocalTopicListener<Event> {
  private final TypeRef<Event> eventType;
  private final Consumer<Event> callback;

  LocalTopicListener(TypeRef<Event> eventType, Consumer<Event> callback) {
    this.eventType = eventType;
    this.callback = callback;
  }

  void dispatch(String bodyJson) {
    try {
      callback.accept(JsonCodec.decode(eventType, bodyJson));
    } catch (Throwable ignored) {
    }
  }
}

final class RuntimeSupport {
  private RuntimeSupport() {
  }

  static String errorText(Throwable error) {
    if (error instanceof ChannelException channelException && channelException.getMessage() != null) {
      return channelException.getMessage();
    }
    if (error.getMessage() != null && !error.getMessage().isEmpty()) {
      return error.getMessage();
    }
    return error.getClass().getSimpleName();
  }
}
