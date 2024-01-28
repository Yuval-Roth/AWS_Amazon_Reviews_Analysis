public record Job(int jobId, Action action, String input) {
    enum Action {
        NONE,
        PROCESS,
        SHUTDOWN,
        DONE
    }
}
