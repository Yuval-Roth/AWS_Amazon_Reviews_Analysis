public record Job(int jobIdCounter, Action action, String input) {
    enum Action {
        NONE,
        PROCESS,
        SHUTDOWN
    }
}
