package v57.channel;

@FunctionalInterface
public interface PostHandler<State, Req, Resp, Ctx> {
  Resp handle(PostRequest<State, Req, Ctx> request) throws Exception;
}
