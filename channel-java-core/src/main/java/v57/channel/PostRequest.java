package v57.channel;

public record PostRequest<State, Req, Ctx>(
    Req body,
    Ctx context,
    Sender<State> sender,
    State state,
    String path
) {
}
