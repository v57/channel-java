package v57.channel;

public record SubscriptionEvent<Event>(String topic, Event body) {
}
