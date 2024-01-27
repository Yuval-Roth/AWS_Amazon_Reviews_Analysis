
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class WorkerMain {

    private static SentimentAnalysisHandler sentimentAnalysisHandler;
    private static NamedEntityRecognitionHandler namedEntityRecognitionHandler;
    private static AtomicBoolean isRunning;
    private static String IN_QUEUE_URL;
    private static String OUT_QUEUE_URL;
    private static String MESSAGE_GROUP_ID;
    private static String MESSAGE_DEDUPLICATION_ID;
    private static SqsClient sqs;
    private static Semaphore sqsLock;

    public static void main(String[] args){

        if(args.length != 4){
            System.out.println("Usage: WorkerMain <in_queue_url> <out_queue_url> <message_group_id> <message_deduplication_id>");
            System.exit(1);
        }

        IN_QUEUE_URL = args[0];
        OUT_QUEUE_URL = args[1];
        MESSAGE_GROUP_ID = args[2];
        MESSAGE_DEDUPLICATION_ID = args[3];

        sqs = SqsClient.builder()
                .region(Region.US_EAST_1)
                .build();
        sqsLock = new Semaphore(1,true);

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

        ReceiveMessageRequest messageRequest = ReceiveMessageRequest.builder()
                .maxNumberOfMessages(1)
                .queueUrl(IN_QUEUE_URL)
                .build();

        Random rand = new Random();

        // Main loop
        while(isRunning.get()){

            // ensure only one thread is accessing the queue at a time
            // this is done to prevent multiple threads from receiving a shutdown message
            try {
                sqsLock.acquire();
            } catch (InterruptedException ignored) {}
            if(!isRunning.get()) break; // Check if another thread received a shutdown message

            // Receive message from in queue
            var request = sqs.receiveMessage(messageRequest);
            if(request.hasMessages()){

                // Deserialize job
                Job job = JsonUtils.deserialize(request.messages().getFirst().body(),Job.class);

                switch(job.action()){
                    case PROCESS -> {
                        sqsLock.release();

                        // Process message and send to out queue
                        String output = processMessage(job.input());
                        String deDupeId = "%s-%s-%s".formatted(MESSAGE_DEDUPLICATION_ID, Thread.currentThread().getName(), rand.nextInt());
                        sqs.sendMessage(SendMessageRequest.builder()
                                .queueUrl(OUT_QUEUE_URL)
                                .messageBody(output)
                                .messageGroupId(MESSAGE_GROUP_ID)
                                .messageDeduplicationId(deDupeId)
                                .build());
                    }
                    case SHUTDOWN -> {
                        isRunning.set(false);
                        sqsLock.release();
                    }
                    case NONE -> sqsLock.release(); // mostly for debugging purposes
                }

                // Delete message from in queue
                sqs.deleteMessage(software.amazon.awssdk.services.sqs.model.DeleteMessageRequest.builder()
                        .queueUrl(IN_QUEUE_URL)
                        .receiptHandle(request.messages().getFirst().receiptHandle())
                        .build());

            } else { // No messages
                sqsLock.release();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static String processMessage(String input) {

        // Deserialize reviews
        TitleReviews tr = JsonUtils.deserialize(input,TitleReviews.class);

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

        // Serialize reviews and return
        return JsonUtils.serialize(tr);
    }
}
