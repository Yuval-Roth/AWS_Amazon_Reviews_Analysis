
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class WorkerMain {

    private static SentimentAnalysisHandler sentimentAnalysisHandler;
    private static NamedEntityRecognitionHandler namedEntityRecognitionHandler;
    private static AtomicBoolean isRunning;
    private static String IN_QUEUE_URL;
    private static String OUT_QUEUE_URL;
    private static String MESSAGE_GROUP_ID;
    private static SqsClient sqs;
    private static Random rand;

    public static void main(String[] args){

        IN_QUEUE_URL = args[0];
        OUT_QUEUE_URL = args[1];
        MESSAGE_GROUP_ID = args[2];
        rand = new Random();

        sqs = SqsClient.builder()
                .region(Region.US_EAST_1)
                .build();

        // BASE ASSUMPTION: This code is running on a machine with 2 vCPUs
        Thread t;

        // init sentiment analysis handler and Entity recognition handler
        // this is done on two threads to save time
        t = new Thread(()-> sentimentAnalysisHandler = SentimentAnalysisHandler.getInstance());
        t.start();
        namedEntityRecognitionHandler = NamedEntityRecognitionHandler.getInstance();
        try {
            t.join();
        } catch (InterruptedException ignored) {}

        // start main loop
        isRunning = new AtomicBoolean(true);
        t = new Thread(WorkerMain::mainLoop);
        t.start();
        mainLoop();

        try {
            t.join();
        } catch (InterruptedException ignored) {}
    }

    private static void mainLoop() {
        while(isRunning.get()){
            
            // Receive message from in queue
            var request =  sqs.receiveMessage(ReceiveMessageRequest.builder()
                    .maxNumberOfMessages(1)
                    .queueUrl(IN_QUEUE_URL)
                    .build());

            if(request.hasMessages()){

                // Process message and then delete from in queue
                processMessage(request.messages().getFirst().body());
                sqs.deleteMessage(software.amazon.awssdk.services.sqs.model.DeleteMessageRequest.builder()
                        .queueUrl(IN_QUEUE_URL)
                        .receiptHandle(request.messages().getFirst().receiptHandle())
                        .build());
            } else {

                // Sleep for 1 second if no messages
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static void processMessage(String message) {

        // Deserialize job
        Job job = JsonUtils.deserialize(message,Job.class);

        if(job.action() == Job.Action.PROCESS){

            // Deserialize reviews
            TitleReviews tr = JsonUtils.deserialize(job.input(),TitleReviews.class);

            // Process reviews
            for(Review r : tr.reviews()){

                // Analyze sentiment and entities
                int sentiment = sentimentAnalysisHandler.findSentiment(r.text());
                Map<String,String> entities = namedEntityRecognitionHandler.findEntities(r.text())
                        .entrySet().stream()
                        .filter(e -> !e.getValue().equals("O")) // Remove non entities
                        .collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));

                // Update review with sentiment and entities
                r.setEntities(entities);
                r.setSentiment(Review.Sentiment.values()[sentiment]);
            }

            // Serialize reviews and send to out queue
            String outMessage = JsonUtils.serialize(tr);
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(OUT_QUEUE_URL)
                    .messageBody(outMessage)
                    .messageGroupId(MESSAGE_GROUP_ID)
                    .messageDeduplicationId(String.valueOf(rand.nextInt()))
                    .build());

        } else if(job.action() == Job.Action.SHUTDOWN){
            isRunning.set(false);
        }
    }
}
