import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public final class ClientRequest {
    private final String clientId;
    private final int requestId;
    private final String fileName;
    private final int reviewsPerWorker;
    private final boolean terminate;
    private  List<TitleReviews> output;
    private  int numJobs;
    private  int reviewsCount;


    public ClientRequest() {
        this.clientId = null;
        this.requestId = 0;
        this.fileName = null;
        this.reviewsPerWorker = 0;
        this.terminate = false;
        this.output = new LinkedList<>();
        this.numJobs = 0;
        this.reviewsCount = 0;
    }

    public ClientRequest(String clientId, int requestId, String fileName, int reviewsPerWorker, boolean terminate) {
        this.clientId = clientId;
        this.requestId = requestId;
        this.fileName = fileName;
        this.reviewsPerWorker = reviewsPerWorker;
        this.terminate = terminate;
        this.output = new LinkedList<>();
        this.numJobs = 0;
        this.reviewsCount = 0;

    }

    public void addTitleReviews(TitleReviews tr) {
        for (TitleReviews o : output) {
            if (o.title().equals(tr.title())) {
                o.reviews().addAll(tr.reviews());
                return;
            }
        }
        output.add(tr);
    }

    public void incrementNumJobs() {
        numJobs++;
    }

    public void decrementNumJobs() {
        numJobs--;
    }

    public boolean isDone() {
        return numJobs == 0;
    }

    public String getProcessedReviewsAsJsons() {
        StringBuilder sb = new StringBuilder();
        for (TitleReviews tr : output) {
            sb.append(JsonUtils.serialize(tr)).append('\n');
        }
        sb.deleteCharAt(sb.length() - 1); // remove last newline
        return sb.toString();
    }

    public int requiredWorkers() {
        return (int) Math.ceil((double) reviewsCount / reviewsPerWorker);
    }

    public void setReviewsCount(int reviewsCount) {
        this.reviewsCount = reviewsCount;
    }

    public CompletedClientRequest getCompletedRequest(String fileName) {
        return new CompletedClientRequest(clientId, requestId, fileName);
    }

    public String clientId() {
        return clientId;
    }

    public int requestId() {
        return requestId;
    }

    public String fileName() {
        return fileName;
    }

    public int reviewsPerWorker() {
        return reviewsPerWorker;
    }

    public boolean terminate() {
        return terminate;
    }

    public List<TitleReviews> output() {
        return output;
    }

    public int numJobs() {
        return numJobs;
    }

    public int reviewsCount() {
        return reviewsCount;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ClientRequest) obj;
        return Objects.equals(this.clientId, that.clientId) &&
                this.requestId == that.requestId &&
                Objects.equals(this.fileName, that.fileName) &&
                this.reviewsPerWorker == that.reviewsPerWorker &&
                this.terminate == that.terminate &&
                Objects.equals(this.output, that.output) &&
                Objects.equals(this.numJobs, that.numJobs) &&
                Objects.equals(this.reviewsCount, that.reviewsCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, requestId, fileName, reviewsPerWorker, terminate, output, numJobs, reviewsCount);
    }

    @Override
    public String toString() {
        return "ClientRequest[" +
                "clientId=" + clientId + ", " +
                "requestId=" + requestId + ", " +
                "fileName=" + fileName + ", " +
                "reviewsPerWorker=" + reviewsPerWorker + ", " +
                "terminate=" + terminate + ", " +
                "numJobs=" + numJobs + ", " +
                "reviewsCount=" + reviewsCount + ']';
    }

}
