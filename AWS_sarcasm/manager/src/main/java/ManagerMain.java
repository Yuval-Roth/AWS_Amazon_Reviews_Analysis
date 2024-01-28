
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class ManagerMain {
    private static final String SQS_DOMAIN_PREFIX = "https://sqs.us-east-1.amazonaws.com/057325794177/";
    private static Region ec2_region = Region.US_EAST_1;
    private static Region s3_region = Region.US_WEST_2;
    private static int jobIdCounter = 0 ;

    public static void main(String[] args) {
    }

    private static void tempMain() {
        String queueName = "user-input";
        ReceiveMessageRequest messageRequest = ReceiveMessageRequest.builder()
                .maxNumberOfMessages(1)
                .queueUrl(SQS_DOMAIN_PREFIX + queueName + ".fifo")
                .build();

        SqsClient sqs = SqsClient.builder()
                .region(ec2_region)
                .build();

        var r = sqs.receiveMessage(messageRequest);
        String message = r.messages().getFirst().body();
        String[] messages = message.split("\n");
        List<TitleReviews> smallTitleReviewsList = new LinkedList<>(); //will hold all title Reviews with 5 reviews
        for (String json : messages) {
            int counterReviews = 0;
            TitleReviews tr = JsonUtils.deserialize(json, TitleReviews.class);
            LinkedList<Review> fiveReviews = new LinkedList<>();
            TitleReviews smallTr = new TitleReviews(tr.title(),fiveReviews);
            for (Review rev: tr.reviews()) {
                if(counterReviews<5){
                    fiveReviews.add(rev);
                    counterReviews++;
                }
                else{ //counterReview == 5
                    smallTitleReviewsList.add(smallTr);
                    fiveReviews = new LinkedList<>();
                    smallTr = new TitleReviews(tr.title(),fiveReviews);
                    counterReviews=0;
                }
            }
        }
        for(TitleReviews tr : smallTitleReviewsList){
            String jsonJob = JsonUtils.serialize(tr);
            Job job = new Job(jobIdCounter, Job.Action.PROCESS,jsonJob);
            String messageBody = JsonUtils.serialize(job);
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(SQS_DOMAIN_PREFIX  + queueName + ".fifo")
                    .messageBody(messageBody)
                    .messageGroupId("1")
                    .messageDeduplicationId(String.valueOf(new Random().nextInt()))
                    .build());
            jobIdCounter++;
        }
    }
}



