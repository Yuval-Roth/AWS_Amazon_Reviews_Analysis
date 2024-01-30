
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

public class mainWorkerClass {

    private static SentimentAnalysisHandler sentimentAnalysisHandler;
    private static NamedEntityRecognitionHandler namedEntityRecognitionHandler;
    private static String IN_QUEUE_URL;
    private static String OUT_QUEUE_URL;
    private static String MANAGER_QUEUE_URL;
    private static String BUCKET_NAME;
    private static String WORKER_ID;
    private static SqsClient sqs;
    private static Semaphore managerQueueLock;
    private static Thread thread2;

    public static void main(String[] args){

        if(args.length != 5){
            System.out.println("Usage: WorkerMain <worker_id> <in_queue_name> <out_queue_name> <manager_queue_name> <bucket_name>");
            System.exit(1);
        }

        WORKER_ID = args[0];
        IN_QUEUE_URL = args[1];
        OUT_QUEUE_URL = args[2];
        MANAGER_QUEUE_URL = args[3];
        BUCKET_NAME = args[4];

        sqs = SqsClient.builder()
                .region(Region.US_EAST_1)
                .build();
        managerQueueLock = new Semaphore(1);

        // BASE ASSUMPTION: This code is running on a machine with 2 vCPUs

        // init sentiment analysis handler and Entity recognition handler
        // this is done on two threads to save time
        thread2 = new Thread(() -> sentimentAnalysisHandler = SentimentAnalysisHandler.getInstance());
        thread2.start();
        namedEntityRecognitionHandler = NamedEntityRecognitionHandler.getInstance();
        try {
            thread2.join();
        } catch (InterruptedException ignored) {}

        final Exception[] exceptionHandler = new Exception[1];
        // start main loop
        while(true) {
            thread2 = new Thread(() -> mainLoop(exceptionHandler));
            thread2.start();
            mainLoop(exceptionHandler);

            try {
                thread2.join();
            } catch (InterruptedException ignored) {}
            if(exceptionHandler[0] != null){
                handleException(exceptionHandler[0]);
                exceptionHandler[0] = null;
            }
        }
    }

    private static void mainLoop(Exception[] exceptionHandler) {

        ReceiveMessageRequest messageRequest = ReceiveMessageRequest.builder()
                .maxNumberOfMessages(1)
                .queueUrl(IN_QUEUE_URL)
                .build();

        // Main loop
        while(exceptionHandler[0] != null){
            try{
                // Receive message from in queue
                var request = sqs.receiveMessage(messageRequest);
                if(request.hasMessages()){

                    // Deserialize job
                    Job job = JsonUtils.deserialize(request.messages().getFirst().body(),Job.class);

                    if(job.action() == Job.Action.PROCESS) {

                        // Process message and send to out queue
                        String output = processMessage(job.input());
                        Job doneJob = new Job(job.jobId(), Job.Action.DONE, output);

                        sqs.sendMessage(SendMessageRequest.builder()
                                .queueUrl(OUT_QUEUE_URL)
                                .messageBody(JsonUtils.serialize(doneJob))
                                .build());
                    }

                    // Delete message from in queue
                    sqs.deleteMessage(software.amazon.awssdk.services.sqs.model.DeleteMessageRequest.builder()
                            .queueUrl(IN_QUEUE_URL)
                            .receiptHandle(request.messages().getFirst().receiptHandle())
                            .build());

                } else {
                    if(exceptionHandler[0] != null){
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                checkForShutdown();
            } catch (Exception e){
                exceptionHandler[0] = e;
                return;
            }
        }
    }

    private static void checkForShutdown() throws TerminateException {

        // ensure only one thread is accessing the manager queue at a time
        // this is done to prevent multiple threads from receiving a shutdown message
        if(managerQueueLock.tryAcquire()){
            var request = sqs.receiveMessage(ReceiveMessageRequest.builder()
                    .maxNumberOfMessages(1)
                    .queueUrl(MANAGER_QUEUE_URL)
                    .build());
            if(request.hasMessages()){
                Job job = JsonUtils.deserialize(request.messages().getFirst().body(),Job.class);
                if(job.action() != Job.Action.SHUTDOWN){
                    throw new IllegalStateException("Expected shutdown message, got: "+job.action());
                }
                sqs.deleteMessage(software.amazon.awssdk.services.sqs.model.DeleteMessageRequest.builder()
                        .queueUrl(MANAGER_QUEUE_URL)
                        .receiptHandle(request.messages().getFirst().receiptHandle())
                        .build());

                throw new TerminateException();
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

    private static void handleException(Exception e) {

        if(e instanceof TerminateException){
            System.exit(0);
        }

        String stackTrace = stackTraceToString(e);
        String logName = "errors/error_worker%s_%s.log".formatted(WORKER_ID, UUID.randomUUID());
        uploadToS3(logName,stackTrace);
    }

    private static void uploadToS3(String key, String content) {
        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();

        s3.putObject(PutObjectRequest.builder()
                        .bucket(BUCKET_NAME)
                        .key("files/"+key).build(),
                RequestBody.fromString(content));

        s3.close();
    }

    private static String stackTraceToString(Exception e) {
        StringBuilder output  = new StringBuilder();
        output.append(e).append("\n");
        for (var element: e.getStackTrace()) {
            output.append("\t").append(element).append("\n");
        }
        return output.toString();
    }
}
