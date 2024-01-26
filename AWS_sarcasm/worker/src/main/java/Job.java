public record Job(Action action, String input) {
    enum Action {
        NONE,
        PROCESS,
        SHUTDOWN
    }
}
