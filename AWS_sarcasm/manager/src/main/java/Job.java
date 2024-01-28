public record Job(int jobId, Action action, String data) {
    enum Action {
        NONE,
        PROCESS,
        SHUTDOWN,
        DONE
    }
}
