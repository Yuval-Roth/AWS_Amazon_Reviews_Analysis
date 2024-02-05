public record Job(int jobId, Action action, String data, boolean highPriority) {

    public Job(int jobId, Action action, String data) {
        this(jobId, action, data, false);
    }
    public enum Action {
        NONE,
        PROCESS,
        SHUTDOWN,
        DONE
    }
}
