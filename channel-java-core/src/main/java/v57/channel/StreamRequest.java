package v57.channel;

public record StreamRequest<State, Req, Ctx>(
    Req body,
    Ctx context,
    Sender<State> sender,
    State state,
    String path
) {
}
