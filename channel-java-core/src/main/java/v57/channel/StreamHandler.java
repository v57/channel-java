package v57.channel;

@FunctionalInterface
public interface StreamHandler<State, Req, Item, Ctx> {
  ChannelStream<Item> handle(StreamRequest<State, Req, Ctx> request) throws Exception;
}
