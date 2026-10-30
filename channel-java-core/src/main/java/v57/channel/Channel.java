package v57.channel;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

public final class Channel<State> {
  private static final TypeRef<Void> VOID = JsonCodec.VOID;

  private final Map<String, PostBinding<State>> posts = new LinkedHashMap<>();
  private final Map<String, StreamBinding<State>> streams = new LinkedHashMap<>();
  private final Map<String, Subscription<?, ?>> subscriptions = new LinkedHashMap<>();
  private final java.util.List<OtherPostBinding<State>> otherPosts = new java.util.ArrayList<>();
  private final java.util.List<OtherStreamBinding<State>> otherStreams = new java.util.ArrayList<>();

  private BiConsumer<State, Sender<State>> onDisconnect;

  public <Resp> Channel<State> post(String path, Class<Resp> responseType, PostHandler<State, Void, Resp, Void> handler) {
    return post(path, VOID, TypeRef.of(responseType), VOID, handler);
  }

  public <Req, Resp> Channel<State> post(
      String path,
      Class<Req> requestType,
      Class<Resp> responseType,
      PostHandler<State, Req, Resp, Void> handler
  ) {
    return post(path, TypeRef.of(requestType), TypeRef.of(responseType), VOID, handler);
  }

  public <Req, Resp, Ctx> Channel<State> post(
      String path,
      TypeRef<Req> requestType,
      TypeRef<Resp> responseType,
      TypeRef<Ctx> contextType,
      PostHandler<State, Req, Resp, Ctx> handler
  ) {
    posts.put(path, new PostBinding<>(requestType, responseType, contextType, handler));
    return this;
  }

  public <Resp> Channel<State> postOther(String prefix, Class<Resp> responseType, PostHandler<State, Void, Resp, Void> handler) {
    return postOther(path -> path.startsWith(prefix), VOID, TypeRef.of(responseType), VOID, handler);
  }

  public <Req, Resp, Ctx> Channel<State> postOther(
      Predicate<String> matcher,
      TypeRef<Req> requestType,
      TypeRef<Resp> responseType,
      TypeRef<Ctx> contextType,
      PostHandler<State, Req, Resp, Ctx> handler
  ) {
    otherPosts.add(new OtherPostBinding<>(matcher, new PostBinding<>(requestType, responseType, contextType, handler)));
    return this;
  }

  public <Item> Channel<State> stream(String path, Class<Item> itemType, StreamHandler<State, Void, Item, Void> handler) {
    return stream(path, VOID, TypeRef.of(itemType), VOID, handler);
  }

  public <Req, Item> Channel<State> stream(
      String path,
      Class<Req> requestType,
      Class<Item> itemType,
      StreamHandler<State, Req, Item, Void> handler
  ) {
    return stream(path, TypeRef.of(requestType), TypeRef.of(itemType), VOID, handler);
  }

  public <Req, Item, Ctx> Channel<State> stream(
      String path,
      TypeRef<Req> requestType,
      TypeRef<Item> itemType,
      TypeRef<Ctx> contextType,
      StreamHandler<State, Req, Item, Ctx> handler
  ) {
    streams.put(path, new StreamBinding<>(requestType, itemType, contextType, handler));
    return this;
  }

  public <Req, Item, Ctx> Channel<State> streamOther(
      Predicate<String> matcher,
      TypeRef<Req> requestType,
      TypeRef<Item> itemType,
      TypeRef<Ctx> contextType,
      StreamHandler<State, Req, Item, Ctx> handler
  ) {
    otherStreams.add(new OtherStreamBinding<>(matcher, new StreamBinding<>(requestType, itemType, contextType, handler)));
    return this;
  }

  public Channel<State> events(Map<String, ? extends Subscription<?, ?>> events) {
    return events(events, "");
  }

  public Channel<State> events(Map<String, ? extends Subscription<?, ?>> events, String prefix) {
    for (var entry : events.entrySet()) {
      var route = join(prefix, entry.getKey());
      var subscription = entry.getValue();
      subscription.bindRoute(route);
      subscriptions.put(route, subscription);
    }
    return this;
  }

  public Channel<State> merge(Channel<State> other, String prefix) {
    for (var entry : other.posts.entrySet()) {
      posts.put(join(prefix, entry.getKey()), entry.getValue());
    }
    for (var entry : other.streams.entrySet()) {
      streams.put(join(prefix, entry.getKey()), entry.getValue());
    }
    for (var entry : other.subscriptions.entrySet()) {
      subscriptions.put(join(prefix, entry.getKey()), entry.getValue().copyForRoute(join(prefix, entry.getKey())));
    }
    for (var binding : other.otherPosts) {
      otherPosts.add(new OtherPostBinding<>(
          prefixedMatcher(prefix, binding.matcher),
          binding.binding
      ));
    }
    for (var binding : other.otherStreams) {
      otherStreams.add(new OtherStreamBinding<>(
          prefixedMatcher(prefix, binding.matcher),
          binding.binding
      ));
    }
    return this;
  }

  public Channel<State> onDisconnect(BiConsumer<State, Sender<State>> action) {
    this.onDisconnect = action;
    return this;
  }

  public ChannelProcess<State> process(State state) {
    return provider(ProcessTransportProvider.class, "channel-java-process").process(this, state);
  }

  public ChannelClient<State> connect(int port) {
    return connect("ws://127.0.0.1:" + port, new ConnectOptions<>());
  }

  public ChannelClient<State> connect(String address) {
    return connect(address, new ConnectOptions<>());
  }

  /** Connects to {@code ws://host:port} (module {@code channel-java-client}) or {@code quic://host:port} ({@code channel-java-quic}). */
  public ChannelClient<State> connect(String address, ConnectOptions<State> options) {
    return clientProvider(address).connect(this, address, options);
  }

  public ChannelServer<State> listen(int port) {
    return listen("127.0.0.1", port, new ListenOptions<>());
  }

  public ChannelServer<State> listen(int port, ListenOptions<State> options) {
    return listen("127.0.0.1", port, options);
  }

  public ChannelServer<State> listen(String host, int port, ListenOptions<State> options) {
    return provider(ServerTransportProvider.class, "channel-java-server").listen(this, host, port, options);
  }

  /** Serves this channel over QUIC on 127.0.0.1 (module {@code channel-java-quic}); {@code options} must carry a certificate. */
  public ChannelServer<State> listenQuic(int port, ListenOptions<State> options) {
    return listenQuic("127.0.0.1", port, options);
  }

  public ChannelServer<State> listenQuic(String host, int port, ListenOptions<State> options) {
    return provider(QuicServerTransportProvider.class, "channel-java-quic").listenQuic(this, host, port, options);
  }

  PostBinding<State> lookupPost(String path) {
    var exact = posts.get(path);
    if (exact != null) {
      return exact;
    }
    for (var other : otherPosts) {
      if (other.matcher.test(path)) {
        return other.binding;
      }
    }
    return null;
  }

  StreamBinding<State> lookupStream(String path) {
    var exact = streams.get(path);
    if (exact != null) {
      return exact;
    }
    for (var other : otherStreams) {
      if (other.matcher.test(path)) {
        return other.binding;
      }
    }
    return null;
  }

  Subscription<?, ?> lookupSubscription(String path) {
    return subscriptions.get(path);
  }

  Map<String, Subscription<?, ?>> subscriptions() {
    return subscriptions;
  }

  void invokeDisconnect(State state, Sender<State> sender) {
    if (onDisconnect == null) {
      return;
    }
    try {
      onDisconnect.accept(state, sender);
    } catch (Throwable ignored) {
    }
  }

  <Req, Resp, Ctx> CallResult<Resp> tryDirectProcessSend(
      Peer<State> peer,
      TypeRef<Req> requestType,
      TypeRef<Resp> responseType,
      TypeRef<Ctx> contextType,
      String path,
      Req body,
      Ctx context
  ) {
    var binding = lookupPost(path);
    if (binding == null) {
      return CallResult.remoteErr("api not found");
    }
    if (!binding.matches(requestType, responseType, contextType)) {
      return null;
    }

    try {
      var sender = new Sender<>(peer);
      var result = binding.invokeDirect(stateOf(peer), sender, path, body, context);
      @SuppressWarnings("unchecked")
      var cast = (Resp) result;
      return CallResult.ok(cast);
    } catch (Throwable error) {
      return CallResult.remoteErr(RuntimeSupport.errorText(error));
    }
  }

  @SuppressWarnings("unchecked")
  private State stateOf(Peer<State> peer) {
    return peer.state();
  }

  private static <T> T provider(Class<T> type, String moduleName) {
    return ServiceLoader.load(type)
        .findFirst()
        .orElseThrow(() -> new ChannelException("missing transport module: " + moduleName));
  }

  private static ClientTransportProvider clientProvider(String address) {
    var separator = address == null ? -1 : address.indexOf("://");
    var scheme = separator < 0 ? "" : address.substring(0, separator).toLowerCase(java.util.Locale.ROOT);
    ClientTransportProvider first = null;
    for (var provider : ServiceLoader.load(ClientTransportProvider.class)) {
      if (provider.supports(scheme)) {
        return provider;
      }
      if (first == null) {
        first = provider;
      }
    }
    if ("quic".equals(scheme)) {
      throw new ChannelException("missing transport module: channel-java-quic");
    }
    if (first == null || "ws".equals(scheme)) {
      throw new ChannelException("missing transport module: channel-java-client");
    }
    // Lets the transport reject the address with its own message
    return first;
  }

  private static Predicate<String> prefixedMatcher(String prefix, Predicate<String> matcher) {
    if (prefix == null || prefix.isEmpty()) {
      return matcher;
    }
    var normalized = prefix + "/";
    return path -> path.startsWith(normalized) && matcher.test(path.substring(normalized.length()));
  }

  static String join(String prefix, String path) {
    if (prefix == null || prefix.isEmpty()) {
      return path;
    }
    if (path == null || path.isEmpty()) {
      return prefix;
    }
    return prefix + "/" + path;
  }

  static final class PostBinding<State> {
    private final TypeRef<?> requestType;
    private final TypeRef<?> responseType;
    private final TypeRef<?> contextType;
    private final PostHandler<State, Object, Object, Object> handler;

    @SuppressWarnings("unchecked")
    private <Req, Resp, Ctx> PostBinding(
        TypeRef<Req> requestType,
        TypeRef<Resp> responseType,
        TypeRef<Ctx> contextType,
        PostHandler<State, Req, Resp, Ctx> handler
    ) {
      this.requestType = requestType;
      this.responseType = responseType;
      this.contextType = contextType;
      this.handler = (PostHandler<State, Object, Object, Object>) handler;
    }

    boolean matches(TypeRef<?> requestType, TypeRef<?> responseType, TypeRef<?> contextType) {
      return this.requestType.type().equals(requestType.type())
          && this.responseType.type().equals(responseType.type())
          && this.contextType.type().equals(contextType.type());
    }

    Object invokeFromJson(State state, Sender<State> sender, String path, String bodyJson, String contextJson) throws Exception {
      var body = decodeValue(requestType, bodyJson);
      var context = decodeValue(contextType, contextJson);
      return handler.handle(new PostRequest<>(body, context, sender, state, path));
    }

    Object invokeDirect(State state, Sender<State> sender, String path, Object body, Object context) throws Exception {
      return handler.handle(new PostRequest<>(body, context, sender, state, path));
    }

    String encodeResponse(Object value) {
      return JsonCodec.encodeNullable(responseType, value);
    }
  }

  static final class StreamBinding<State> {
    private final TypeRef<?> requestType;
    private final TypeRef<?> itemType;
    private final TypeRef<?> contextType;
    private final StreamHandler<State, Object, Object, Object> handler;

    @SuppressWarnings("unchecked")
    private <Req, Item, Ctx> StreamBinding(
        TypeRef<Req> requestType,
        TypeRef<Item> itemType,
        TypeRef<Ctx> contextType,
        StreamHandler<State, Req, Item, Ctx> handler
    ) {
      this.requestType = requestType;
      this.itemType = itemType;
      this.contextType = contextType;
      this.handler = (StreamHandler<State, Object, Object, Object>) handler;
    }

    ChannelStream<?> openFromJson(State state, Sender<State> sender, String path, String bodyJson, String contextJson) throws Exception {
      var body = decodeValue(requestType, bodyJson);
      var context = decodeValue(contextType, contextJson);
      return handler.handle(new StreamRequest<>(body, context, sender, state, path));
    }

    TypeRef<?> itemType() {
      return itemType;
    }
  }

  private record OtherPostBinding<State>(Predicate<String> matcher, PostBinding<State> binding) {
  }

  private record OtherStreamBinding<State>(Predicate<String> matcher, StreamBinding<State> binding) {
  }

  @SuppressWarnings("unchecked")
  private static <T> T decodeValue(TypeRef<?> type, String json) {
    return (T) JsonCodec.decode((TypeRef<Object>) type, json);
  }
}
