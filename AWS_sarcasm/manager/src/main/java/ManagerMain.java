
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ManagerMain {
    private static final String SQS_DOMAIN_PREFIX = "https://sqs.us-east-1.amazonaws.com/057325794177/";
    private static Region ec2_region = Region.US_EAST_1;
    private static Region s3_region = Region.US_WEST_2;

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
        List<TitleReviews> titleReviewsList = new LinkedList<>(); //will hold all title Reviews with 5 reviews
        int counterReviews = 0;
        int reviewsSize = 5;
        for (String json : messages) {
            TitleReviews tr = JsonUtils.deserialize(json, TitleReviews.class);
            Review[] fiveReviews = new Review[reviewsSize];
            TitleReviews smallTr = new TitleReviews(tr.title(),fiveReviews);
            for (Review rev: tr.reviews()) {
                if(counterReviews<5){
                    fiveReviews[counterReviews] = rev;
                    counterReviews++;
                }
                else{ //counterReview == 5
                    titleReviewsList.add(smallTr);
                    fiveReviews = new Review[reviewsSize];
                    smallTr = new TitleReviews(tr.title(),fiveReviews);
                    counterReviews=0;
                }
            }
        }

    }
}



