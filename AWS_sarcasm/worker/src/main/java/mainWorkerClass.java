
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

public class mainWorkerClass {

    // <DEBUG FLAGS>
    private static final String USAGE = """
                Usage: java -jar managerProgram.jar -workerId <id> -inQueueUrl <url> -outQueueUrl <url>
                -managerQueueUrl <url> -S3BucketName <name>  [-h | -help] [optional debug flags]
                                    
                -h | -help :- Print this message and exit.
                                    
                optional debug flags:
                
                    -d | -debug :- Run in debug mode, logging all operations to standard output
                    
                    -ul | -uploadLog <file name> :- logs will be uploaded to <file name> in the S3 bucket.
                                  Must be used with -debug.
                                  
                    -ui | -uploadInterval <interval in seconds> :- When combined with -uploadLog, specifies the interval in seconds
                                  between log uploads to the S3 bucket.
                                  Must be a positive integer, must be used with -uploadLog.
                                  If this argument is not specified, defaults to 60 seconds.
                """;
    private static volatile boolean debugMode;
    private static volatile boolean uploadLogs;
    private static volatile int appendLogIntervalInSeconds;
    private static volatile StringBuilder uploadBuffer;
    private static volatile long nextLogUpload;
    private static String uploadLogName;
    private static Semaphore logUploadLock;
    // </DEBUG FLAGS>

    private static SentimentAnalysisHandler sentimentAnalysisHandler;
    private static NamedEntityRecognitionHandler namedEntityRecognitionHandler;
    private static String IN_QUEUE_URL;
    private static String OUT_QUEUE_URL;
    private static String MANAGER_QUEUE_URL;
    private static String BUCKET_NAME;
    private static String WORKER_ID;
    private static SqsClient sqs;
    private static S3Client s3;
    private static Semaphore managerQueueLock;
    private static Thread thread2;

    public static void main(String[] args){


        uploadBuffer = new StringBuilder();
        readArgs(args);

        sqs = SqsClient.builder()
                .region(Region.US_EAST_1)
                .build();
        s3 = S3Client.builder()
                .region(Region.US_WEST_2)
                .build();
        managerQueueLock = new Semaphore(1);
        logUploadLock = new Semaphore(1);

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
            thread2 = new Thread(() -> mainLoop(exceptionHandler),"secondary");
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

        nextLogUpload = System.currentTimeMillis() + appendLogIntervalInSeconds * 1000L;

        // Main loop
        while(exceptionHandler[0] == null){
            try{
                // Receive message from in queue
                var request = sqs.receiveMessage(messageRequest);
                if(request.hasMessages()){

                    // Deserialize job
                    Job job = JsonUtils.deserialize(request.messages().getFirst().body(),Job.class);

                    if(job.action() == Job.Action.PROCESS) {

                        // Process message and send to out queue
                        long start = System.currentTimeMillis();
                        log("thread %s started processing job: %s".formatted(Thread.currentThread().getName(),job.jobId()));
                        String output = processMessage(job.data());
                        log("thread %s finished processing job: %s in %s ms".formatted(Thread.currentThread().getName(),job.jobId(),System.currentTimeMillis()-start));
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
                    if(exceptionHandler[0] == null){
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }

                if(uploadLogs && System.currentTimeMillis() > nextLogUpload && logUploadLock.tryAcquire()){
                    uploadBuffer.append("\n");
                    appendToS3(uploadLogName,uploadBuffer.toString());
                    uploadBuffer = new StringBuilder();
                    nextLogUpload = System.currentTimeMillis() + appendLogIntervalInSeconds * 1000L;
                    logUploadLock.release();
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
            log("thread %s found review: %s".formatted(Thread.currentThread().getName(),r.text()));

            long start = System.currentTimeMillis();
            log("thread %s started sentiment analysis".formatted(Thread.currentThread().getName()));
            // Analyze sentiment and entities
            int sentiment = sentimentAnalysisHandler.findSentiment(r.text());

            log("thread %s finished sentiment analysis in %s ms".formatted(Thread.currentThread().getName(),System.currentTimeMillis()-start));

            start = System.currentTimeMillis();
            log("thread %s started named entity recognition".formatted(Thread.currentThread().getName()));
            Map<String,String> entities = namedEntityRecognitionHandler.findEntities(r.text())
                    .entrySet().stream()
                    .filter(e -> !e.getValue().equals("O")) // Remove non entities
                    .collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));

            log("thread %s finished named entity recognition in %s ms".formatted(Thread.currentThread().getName(),System.currentTimeMillis()-start));

            // Update review with sentiment and entities
            r.setEntities(entities);
            r.setSentiment(Review.Sentiment.values()[sentiment]);
        }

        // Serialize reviews and return
        return JsonUtils.serialize(tr);
    }

    private static void handleException(Exception e) {

        if(e instanceof TerminateException){
            if(uploadLogs && ! uploadBuffer.isEmpty()){
                appendToS3("logs/"+uploadLogName, uploadBuffer.toString());
            }
            System.exit(0);
        }

        String stackTrace = stackTraceToString(e);
        String logName = "errors/error_worker%s_%s.log".formatted(WORKER_ID, UUID.randomUUID());
        uploadToS3(logName,stackTrace);
    }

    private static String downloadFromS3(String key) {
        var r = s3.getObject(GetObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key("files/"+key).build());

        // get file from response
        byte[] file = {};
        try {
            file = r.readAllBytes();
        } catch (IOException e) {
            handleException(e);
        }

        return new String(file);
    }

    private static void uploadToS3(String key, String content) {
        S3Client s3 = S3Client.builder()
                .region(Region.US_WEST_2)
                .build();

        s3.putObject(PutObjectRequest.builder()
                        .bucket(BUCKET_NAME)
                        .key("files/"+key).build(),
                RequestBody.fromString(content));

        s3.close();
    }

    private static void appendToS3(String key, String content) {
        String oldContent = "";
        try{
            oldContent = downloadFromS3(key);
        } catch (NoSuchKeyException ignored){}
        uploadToS3(key, oldContent + content);
    }

    private static String stackTraceToString(Exception e) {
        StringBuilder output  = new StringBuilder();
        output.append(e).append("\n");
        for (var element: e.getStackTrace()) {
            output.append("\t").append(element).append("\n");
        }
        return output.toString();
    }

    private static void log(String message){
        if(debugMode){
            String timeStamp = getTimeStamp(LocalDateTime.now());
            if(uploadLogs){
                uploadBuffer.append(timeStamp).append(" ").append(message).append("\n");
            }
            System.out.printf("%s %s%n",timeStamp,message);
        }
    }

    private static String getTimeStamp(LocalDateTime now) {
        return "[%s.%s.%s - %s:%s:%s]".formatted(
                now.getDayOfMonth() > 9 ? now.getDayOfMonth() : "0"+ now.getDayOfMonth(),
                now.getMonthValue() > 9 ? now.getMonthValue() : "0"+ now.getMonthValue(),
                now.getYear(),
                now.getHour() > 9 ? now.getHour() : "0"+ now.getHour(),
                now.getMinute() > 9 ? now.getMinute() : "0"+ now.getMinute(),
                now.getSecond() > 9 ? now.getSecond() : "0"+ now.getSecond());
    }

    private static void printUsageAndExit(String errorMessage) {
        if(! errorMessage.equals("")) {
            System.out.println(errorMessage);
        }
        System.out.println(USAGE);
        System.exit(1);
    }

    private static void readArgs(String[] args) {

        if(args.length == 0){
            System.out.println();
            printUsageAndExit("no arguments provided\n");
        }

        List<String> helpOptions = List.of("-h","-help");
        List<String> debugModeOptions = List.of("-d","-debug");
        List<String> uploadLogOptions = List.of("-ul","-uploadlog");
        List<String> uploadIntervalOptions = List.of("-ui","-uploadinterval");
        List<String> argsList = new LinkedList<>();
        argsList.addAll(helpOptions);
        argsList.addAll(debugModeOptions);
        argsList.addAll(uploadLogOptions);
        argsList.addAll(uploadIntervalOptions);
        argsList.add("-workerid");
        argsList.add("-inqueueurl");
        argsList.add("-outqueueurl");
        argsList.add("-managerqueueurl");
        argsList.add("-s3bucketname");

        for (int i = 0; i < args.length; i++) {
            String arg = args[i].toLowerCase();
            String errorMessage;

            if (arg.equals("-workerid")) {
                errorMessage = "Missing worker id\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printUsageAndExit(errorMessage);
                    }
                    WORKER_ID = args[i+1];
                    i++;
                    continue;
                } catch (IndexOutOfBoundsException e){
                    System.out.println();
                    printUsageAndExit(errorMessage);
                }
            }

            if (arg.equals("-inqueueurl")) {
                errorMessage = "Missing in queue url\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printUsageAndExit(errorMessage);
                    }
                    IN_QUEUE_URL = args[i+1];
                    i++;
                    continue;
                } catch (IndexOutOfBoundsException e){
                    System.out.println();
                    printUsageAndExit(errorMessage);
                }
            }

            if (arg.equals("-outqueueurl")) {
                errorMessage = "Missing out queue url\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printUsageAndExit(errorMessage);
                    }
                    OUT_QUEUE_URL = args[i+1];
                    i++;
                    continue;
                } catch (IndexOutOfBoundsException e){
                    System.out.println();
                    printUsageAndExit(errorMessage);
                }
            }

            if (arg.equals("-managerqueueurl")) {
                errorMessage = "Missing manager queue url\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printUsageAndExit(errorMessage);
                    }
                    MANAGER_QUEUE_URL = args[i+1];
                    i++;
                    continue;
                } catch (IndexOutOfBoundsException e){
                    System.out.println();
                    printUsageAndExit(errorMessage);
                }
            }

            if (arg.equals("-s3bucketname")) {
                errorMessage = "Missing S3 bucket name\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printUsageAndExit(errorMessage);
                    }
                    BUCKET_NAME = args[i+1];
                    i++;
                    continue;
                } catch (IndexOutOfBoundsException e){
                    System.out.println();
                    printUsageAndExit(errorMessage);
                }
            }


            if (debugModeOptions.contains(arg)) {
                debugMode = true;
                continue;
            }
            if (uploadLogOptions.contains(arg)) {
                uploadLogs = true;
                errorMessage = "Missing upload log name\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printUsageAndExit(errorMessage);
                    }
                    uploadLogName = args[i+1];
                    i++;
                    continue;
                } catch (IndexOutOfBoundsException e){
                    System.out.println();
                    printUsageAndExit(errorMessage);
                }
            }
            if (uploadIntervalOptions.contains(arg)) {
                errorMessage = "Missing upload interval\n";
                try{
                    if(argsList.contains(args[i+1])){
                        printUsageAndExit(errorMessage);
                    }
                    appendLogIntervalInSeconds = Integer.parseInt(args[i+1]);
                    i++;
                    continue;
                } catch (IndexOutOfBoundsException e){
                    printUsageAndExit(errorMessage);
                } catch (NumberFormatException e){
                    printUsageAndExit("Invalid upload interval\n");
                }
            }
            if (arg.equals("-h") || arg.equals("-help")) {
                printUsageAndExit("");
            }

            System.out.println();
            printUsageAndExit("Unknown argument: %s\n".formatted(arg));
        }

        if(uploadLogs && ! debugMode){
            printUsageAndExit("Upload logs flag was provided but not debug mode flag\n");
        }

        if(uploadLogs && appendLogIntervalInSeconds == 0){
            appendLogIntervalInSeconds = 60;
        }

        log("Manager started");
        log("Upload logs: %s".formatted(uploadLogs));
        log("log name: %s".formatted(uploadLogName));
        log("Upload interval: %s".formatted(appendLogIntervalInSeconds));
    }





}
