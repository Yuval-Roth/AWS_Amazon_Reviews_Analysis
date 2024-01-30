
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class WorkerMain {

    private static SentimentAnalysisHandler sentimentAnalysisHandler;
    private static NamedEntityRecognitionHandler namedEntityRecognitionHandler;
    private static AtomicBoolean isRunning;
    private static String IN_QUEUE_URL;
    private static String OUT_QUEUE_URL;
    private static String MANAGER_QUEUE_URL;
    private static String MESSAGE_GROUP_ID;
    private static String WORKER_ID;
    private static SqsClient sqs;
    private static Semaphore managerQueueLock;

    public static void main(String[] args){

        if(args.length != 4){
            System.out.println("Usage: WorkerMain <worker_id> <in_queue_name> <out_queue_name> <manager_queue_name>");
            System.exit(1);
        }

        WORKER_ID = args[0];
        IN_QUEUE_URL = args[1];
        OUT_QUEUE_URL = args[2];
        MANAGER_QUEUE_URL = args[3];

        sqs = SqsClient.builder()
                .region(Region.US_EAST_1)
                .build();
        managerQueueLock = new Semaphore(1);

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

        // Main loop
        while(isRunning.get()){

            // Receive message from in queue
            var request = sqs.receiveMessage(messageRequest);
            if(request.hasMessages()){

                // Deserialize job
                Job job = JsonUtils.deserialize(request.messages().getFirst().body(),Job.class);

                if(job.action() == Job.Action.PROCESS) {

                    // Process message and send to out queue
                    String output = processMessage(job.input());
                    sqs.sendMessage(SendMessageRequest.builder()
                            .queueUrl(OUT_QUEUE_URL)
                            .messageBody(output)
                            .build());
                }

                // Delete message from in queue
                sqs.deleteMessage(software.amazon.awssdk.services.sqs.model.DeleteMessageRequest.builder()
                        .queueUrl(IN_QUEUE_URL)
                        .receiptHandle(request.messages().getFirst().receiptHandle())
                        .build());

            } else {
                if(isRunning.get()){
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            checkForShutdown();
        }
    }

    private static void checkForShutdown() {

        // ensure only one thread is accessing the manager queue at a time
        // this is done to prevent multiple threads from receiving a shutdown message
        if(managerQueueLock.tryAcquire()){
            var request = sqs.receiveMessage(ReceiveMessageRequest.builder()
                    .maxNumberOfMessages(1)
                    .queueUrl(MANAGER_QUEUE_URL)
                    .build());
            if(request.hasMessages()){
                Job job = JsonUtils.deserialize(request.messages().getFirst().body(),Job.class);
                if(job.action() == Job.Action.SHUTDOWN){
                    isRunning.set(false);
                }
                sqs.deleteMessage(software.amazon.awssdk.services.sqs.model.DeleteMessageRequest.builder()
                        .queueUrl(MANAGER_QUEUE_URL)
                        .receiptHandle(request.messages().getFirst().receiptHandle())
                        .build());

                // don't release lock if shutting down
            } else {
                managerQueueLock.release();
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

    private static String getDeDupeId(){
        return "%s-%s-%s".formatted(WORKER_ID,Thread.currentThread().getName(), UUID.randomUUID());
    }
}
