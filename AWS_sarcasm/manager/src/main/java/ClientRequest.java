import java.util.LinkedList;
import java.util.List;

public record ClientRequest (int requestId, String input, List<TitleReviews> output, Integer[] numJobs){
    public ClientRequest(int requestId, String input){
        this(requestId, input, new LinkedList<>(), new Integer[]{0});
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

    public CompletedClientRequest getCompletedRequest(){

        if(!isDone()){
            throw new IllegalStateException("Cannot get completed request when not done");
        }

        return new CompletedClientRequest(requestId, JsonUtils.serialize(output));
    }

}
