import java.util.LinkedList;
import java.util.List;

public record ClientRequest (
        String clientId,
        int requestId,
        String fileName,
        int reviewsPerWorker,
        boolean terminate,
        List<TitleReviews> output,
        Integer[] numJobs,
        Integer[] reviewsCount
        ){
    public ClientRequest(String clientId, int requestId, String fileName, int reviewsPerWorker, boolean terminate){
        this(clientId, requestId, fileName, reviewsPerWorker, terminate,new LinkedList<>(), new Integer[]{0},new Integer[]{0});
    }

    public void addTitleReviews(TitleReviews tr){
        for(TitleReviews o : output){
            if(o.title().equals(tr.title())){
                o.reviews().addAll(tr.reviews());
                return;
            }
        }
        output.add(tr);
    }

    public void incrementNumJobs(){
        numJobs[0]++;
    }

    public void decrementNumJobs(){
        numJobs[0]--;
    }

    public boolean isDone(){
        return numJobs[0] == 0;
    }
    public String getProcessedReviewsAsJson(){
        StringBuilder sb = new StringBuilder();
        for(TitleReviews tr : output){
            sb.append(JsonUtils.serialize(tr)).append('\n');
        }
        sb.deleteCharAt(sb.length()-1); // remove last newline
        return sb.toString();
    }

    public int requiredWorkers(){
        return (int) Math.ceil((double) output.size() / reviewsPerWorker);
    }

    public void setReviewsCount(int reviewsCount){
        this.reviewsCount[0] = reviewsCount;
    }

    public CompletedClientRequest getCompletedRequest(String fileName) {
        return new CompletedClientRequest(clientId, requestId, fileName);
    }
}
