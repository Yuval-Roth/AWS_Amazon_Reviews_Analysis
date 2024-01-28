
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.LinkedList;
import java.util.List;

public class ManagerMain {
    private static final String SQS_DOMAIN_PREFIX = "https://sqs.us-east-1.amazonaws.com/057325794177/";
    private static Region ec2_region = Region.US_EAST_1;
    private static Region s3_region = Region.US_WEST_2;
    public static void main(String[] args) {
        String queueName = "user-input";
        ReceiveMessageRequest messageRequest = ReceiveMessageRequest.builder()
                .maxNumberOfMessages(1)
                .queueUrl(SQS_DOMAIN_PREFIX + queueName + ".fifo")
                .build();

       SqsClient sqs = SqsClient.builder()
                .region(ec2_region)
                .build();

       var r =  sqs.receiveMessage(messageRequest);
       String message = r.messages().getFirst().body().toString();
       String[] messages = message.split("\n");
        for (String json:messages) {
            int counterReviews  = 0;
            List<TitleReviews> titleReviewsList = new LinkedList<>();
            TitleReviews tr = JsonUtils.deserialize(json,TitleReviews.class);
            for (:
                 ) {
                
            }
            
                    
            

        }





    }


}
