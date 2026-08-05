package v57.channel;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

public final class Subscription<Req, Event> {
  interface Publisher<Event> {
    void publish(SubscriptionEvent<Event> event);
  }

  private final TypeRef<Req> requestType;
  private final TypeRef<Event> eventType;
  private final Function<Req, String> topicResolver;
  private final Function<Req, Object> ackBodyResolver;
  private final List<Publisher<Event>> publishers = new CopyOnWriteArrayList<>();
  private String route = "";

  public Subscription(Class<Req> requestType, Class<Event> eventType) {
    this(TypeRef.of(requestType), TypeRef.of(eventType));
  }

  public Subscription(TypeRef<Req> requestType, TypeRef<Event> eventType) {
    this(requestType, eventType, value -> value == null ? "" : String.valueOf(value), value -> value);
  }

  public Subscription(
      TypeRef<Req> requestType,
      TypeRef<Event> eventType,
      Function<Req, String> topicResolver,
      Function<Req, Object> ackBodyResolver
  ) {
    this.requestType = requestType;
    this.eventType = eventType;
    this.topicResolver = topicResolver;
    this.ackBodyResolver = ackBodyResolver;
  }

  TypeRef<Req> requestType() {
    return requestType;
  }

  TypeRef<Event> eventType() {
    return eventType;
  }

  void bindRoute(String route) {
    this.route = route == null ? "" : route;
  }

  String route() {
    return route;
  }

  String fullTopic(Req request) {
    var suffix = topicResolver.apply(request);
    if (suffix == null || suffix.isEmpty()) {
      return route;
    }
    if (route == null || route.isEmpty()) {
      return suffix;
    }
    return route + "/" + suffix;
  }

  Object ackBody(Req request) {
    return ackBodyResolver.apply(request);
  }

  void addPublisher(Publisher<Event> publisher) {
    publishers.add(publisher);
  }

  void removePublisher(Publisher<Event> publisher) {
    publishers.remove(publisher);
  }

  @SuppressWarnings("unchecked")
  Subscription<Req, Event> copyForRoute(String route) {
    var copy = new Subscription<>(requestType, eventType, topicResolver, ackBodyResolver);
    copy.bindRoute(route);
    return copy;
  }

  public void send(String topic, Event body) {
    var fullTopic = route == null || route.isEmpty()
        ? topic
        : (topic == null || topic.isEmpty() ? route : route + "/" + topic);
    var event = new SubscriptionEvent<>(fullTopic, body);
    for (var publisher : publishers) {
      publisher.publish(event);
    }
  }
}
