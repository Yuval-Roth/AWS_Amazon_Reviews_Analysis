import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ManagerMainClass {

    // <S3>
    public static final String BUCKET_NAME = "distributed-systems-2024-bucket-yuval-adi";
    public static final int BIG_REVIEW_LENGTH = 800;
    private static S3Client s3;
    // </S3>

    // <EC2>
    public static String WORKER_IMAGE_ID;
    public static final String SECURITY_GROUP = "sg-00c67312e0a74a525";
    public static final String WORKER_INSTANCE_TYPE = "t2.large";
    public static final int MANAGER_ID = 1;
    private static Ec2Client ec2;
    private static int instanceIdCounter;
    // </EC2>


    // <SQS>
    private static final String WORKER_MESSAGE_GROUP_ID = "workerGroup";
    private static final String WORKER_IN_QUEUE_NAME = "workerInQueue";
    private static final String WORKER_OUT_QUEUE_NAME = "workerOutQueue";
    private static final String WORKER_MANAGEMENT_QUEUE_NAME = "workerManagementQueue.fifo";
    private static final String USER_INPUT_QUEUE_NAME = "userInputQueue";
    private static final String USER_OUTPUT_QUEUE_NAME = "userOutputQueue";
    private static final String SQS_DOMAIN_PREFIX = "https://sqs.us-east-1.amazonaws.com/057325794177/";
    private static final int MAX_WORKERS = 8;
    private static SqsClient sqs;
    // </SQS>

    // <APPLICATION DATA>

    // <DEBUG FLAGS>
    private static final String USAGE = """
                Usage: java -jar managerProgram.jar [-h | -help] [optional debug flags]
                                    
                -h | -help :- Print this message and exit.
                                    
                optional debug flags:
                
                    -d | -debug :- Run in debug mode, logging all operations to standard output
                    
                    -ul | -uploadLog <file name> :- logs will be uploaded to <file name> in the S3 bucket.
                                  Must be used with -debug.
                                  
                    -ui | -uploadInterval <interval in seconds> :- When combined with -uploadLog, specifies the interval in seconds
                                  between log uploads to the S3 bucket.
                                  Must be a positive integer, must be used with -uploadLog.
                                  If this argument is not specified, defaults to 60 seconds.
                                  
                    -noEc2 :- Run without creating EC2 instances. Useful for debugging locally.
                """;
    private static volatile boolean debugMode;
    private static volatile boolean noEc2;
    private static volatile boolean uploadLogs;
    private static volatile int appendLogIntervalInSeconds;
    private static volatile StringBuilder uploadBuffer;
    private static volatile long nextLogUpload;
    private static String uploadLogName;
    // </DEBUG FLAGS>


    private static final Region ec2_region = Region.US_EAST_1;
    private static final Region s3_region = Region.US_WEST_2;
    private static int jobIdCounter;
    private static volatile Map<Integer,ClientRequest> clientRequestIdToClientRequest;
    private static volatile Map<Integer, Integer> jobIdToClientRequestId;
    private static Semaphore workerCountLock;
    private static volatile long nextClientRequestCheck;
    private static volatile long nextWorkerCountCheck;
    private static AtomicBoolean shouldTerminate;
    private static ThreadPoolExecutor executor;
    private static AtomicInteger requiredWorkers;
    private static final int BATCH_REQUEST_MAX_BYTES = 262144;
    private static final int BATCH_REQUEST_MAX_ENTRIES = 10;
    private static final int WORKER_IN_QUEUE_VISIBILITY_TIMEOUT = 60;
    // </APPLICATION DATA>


    public static void main(String[] args) {

        uploadBuffer = new StringBuilder();
        readArgs(args);

        sqs = SqsClient.builder()
                .region(ec2_region)
                .build();

        s3 = S3Client.builder()
                .region(s3_region)
                .build();

        ec2 = Ec2Client.builder()
                .region(ec2_region)
                .build();

        if(! noEc2){
            var r =  ec2.describeImages(DescribeImagesRequest.builder()
                    .filters(Filter.builder()
                            .name("name")
                            .values("workerImage")
                            .build())
                    .build());
            try{
                WORKER_IMAGE_ID = r.images().getFirst().imageId();
            } catch(NoSuchElementException e){
                log("No worker image found");
                handleException(new TerminateException());
            }
        }

        jobIdToClientRequestId = new HashMap<>();
        clientRequestIdToClientRequest = new HashMap<>();
        jobIdCounter = 0;
        instanceIdCounter = 2;
        workerCountLock = new Semaphore(1);
        shouldTerminate = new AtomicBoolean(false);
        requiredWorkers = new AtomicInteger(0);
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);

        log("Manager started");
        log("Worker image id: %s".formatted(WORKER_IMAGE_ID));
        log("Upload logs: %s".formatted(uploadLogs));
        log("log name: %s".formatted(uploadLogName));
        log("Upload interval: %s".formatted(appendLogIntervalInSeconds));
        log("No EC2: %s".formatted(noEc2));

        createBucketIfNotExists(BUCKET_NAME);
        createQueuesIfNotExists();

        Box<Exception> exceptionHandler = new Box<>(null);

        if(uploadLogs){
            nextLogUpload = System.currentTimeMillis() + (appendLogIntervalInSeconds * 1000L);
        } else {
            nextLogUpload = Long.MAX_VALUE;
        }

        while(true){
            Thread secondaryThread = new Thread(()-> secondaryLoop(exceptionHandler),"secondary");
            secondaryThread.start();
            mainLoop(exceptionHandler);

            try {
                secondaryThread.join();
            } catch (InterruptedException ignored) {}
            if(exceptionHandler.get() != null){
                handleException(exceptionHandler.get());
                exceptionHandler.set(null);
            }
        }
    }

    private static void mainLoop(Box<Exception> exceptionHandler) {

        while(exceptionHandler.get() == null){
            try{
                checkForCompletedJobs(exceptionHandler);
            } catch (Exception e){
                exceptionHandler.set(e);
                return;
            }
        }
    }

    private static void secondaryLoop(Box<Exception> exceptionHandler) {

        long nextWakeup;
        while(exceptionHandler.get() == null){
            try{
                if(! shouldTerminate.get() && System.currentTimeMillis() >= nextClientRequestCheck) {
                    checkForClientRequests(exceptionHandler);
                    nextClientRequestCheck = System.currentTimeMillis() + 3000;
                }

                if(System.currentTimeMillis() >= nextWorkerCountCheck ) {
                    if(workerCountLock.tryAcquire()){
                        balanceInstanceCount(exceptionHandler);
                    }
                    nextWorkerCountCheck = System.currentTimeMillis() + 5000;
                }

                if(uploadLogs && System.currentTimeMillis() >= nextLogUpload){
                    if(! uploadBuffer.isEmpty()){
                        appendToS3("logs/"+uploadLogName, uploadBuffer.toString());
                        uploadBuffer = new StringBuilder();
                    }
                    nextLogUpload = System.currentTimeMillis() + (appendLogIntervalInSeconds * 1000L);
                }

                nextWakeup = min(nextClientRequestCheck, nextWorkerCountCheck, nextLogUpload);

                try {
                    Thread.sleep(Math.max(0,nextWakeup - System.currentTimeMillis()));
                } catch (InterruptedException ignored) {}

            } catch (Exception e){
                exceptionHandler.set(e);
                return;
            }
        }
    }

    // ============================================================================ |
    // ========================  MAIN FLOW FUNCTIONS  ============================= |
    // ============================================================================ |

    private static void checkForClientRequests(Box<Exception> exceptionHandler) {

        ReceiveMessageRequest messageRequest = ReceiveMessageRequest.builder()
                .queueUrl(getQueueURL(USER_INPUT_QUEUE_NAME))
                .waitTimeSeconds(1)
                .build();

        ReceiveMessageResponse r;
        do{
            r = sqs.receiveMessage(messageRequest);

            if(r.hasMessages()){
                handleClientRequests(r.messages(),exceptionHandler);
                ReceiveMessageResponse finalR = r;
                executeLater(()->deleteBatchFromQueue(USER_INPUT_QUEUE_NAME, finalR.messages()),exceptionHandler);
            }
        } while(r.hasMessages());
    }

    private static void handleClientRequests(List<Message> messages, Box<Exception> exceptionHandler){

        for(var message : messages) {

            // read message and create client request
            ClientRequest clientRequest = JsonUtils.deserialize(message.body(), ClientRequest.class);
            clientRequestIdToClientRequest.put(clientRequest.requestId(), clientRequest);
            String input;
            try{
                input = downloadFromS3(clientRequest.fileName());
            } catch(NoSuchKeyException e){
                log("received client request: %s".formatted(clientRequest));
                log("File not found: %s".formatted(clientRequest.fileName()));
                CompletedClientRequest fileNotFound = new CompletedClientRequest(clientRequest.clientId(), clientRequest.requestId(), "File not found");
                executeLater(()->sendToQueue(USER_OUTPUT_QUEUE_NAME, JsonUtils.serialize(fileNotFound)),exceptionHandler);
                continue;
            }

            if(clientRequest.terminate()){
                shouldTerminate.set(true);
            }

            // Deserialize client request fileName and split to small TitleReviews
            String[] jsons = input.split("\n");
            List<TitleReviews> largeTitleReviewsList = new LinkedList<>();
            int reviewsCount = 0;
            for (String json : jsons) {
                TitleReviews tr = JsonUtils.deserialize(json, TitleReviews.class);
                reviewsCount += addTitleReviews(tr, largeTitleReviewsList);
            }
            // set reviews count in client request and add to required workers
            clientRequest.setReviewsCount(reviewsCount);
            addToAtomicInteger(clientRequest.requiredWorkers());

            StringBuilder _attachedJobs = new StringBuilder("Attached jobs: [ "); // logging purposes

            // split TitleReviews to small TitleReviews and create jobs
            List<String> jobs = largeTitleReviewsList.stream()
                    .flatMap(tr -> splitTitleReviews(tr, clientRequest.reviewsPerWorker()).stream())
                    .map(JsonUtils::serialize)
                    .map(trJson -> {
                        int jobId = jobIdCounter++;
                        _attachedJobs.append(jobId).append(" "); // logging purposes
                        jobIdToClientRequestId.put(jobId,clientRequest.requestId());
                        clientRequest.incrementNumJobs();
                        return JsonUtils.serialize(new Job(jobId, Job.Action.PROCESS, trJson));
                    })
                    .toList();

            executeLater(()->sendBatchToQueue(WORKER_IN_QUEUE_NAME, jobs),exceptionHandler);

            _attachedJobs.append("]"); // logging purposes
            log("Received client request: %s\n%s".formatted(clientRequest,_attachedJobs));
        }
    }

    private static void checkForCompletedJobs(Box<Exception> exceptionHandler) throws TerminateException {

        ReceiveMessageRequest messageRequest = ReceiveMessageRequest.builder()
                .queueUrl(getQueueURL(WORKER_OUT_QUEUE_NAME))
                .waitTimeSeconds(20)
                .build();

        ReceiveMessageResponse r = sqs.receiveMessage(messageRequest);

        if(r.hasMessages()) {
            handleCompletedJobs(r.messages());
            ReceiveMessageResponse finalR = r;
            executeLater(()->deleteBatchFromQueue(WORKER_OUT_QUEUE_NAME, finalR.messages()),exceptionHandler);
        }

        if(shouldTerminate.get() && clientRequestIdToClientRequest.isEmpty()){
            log("Terminating");
            throw new TerminateException();
        }
    }

    private static void handleCompletedJobs(List<Message> messages){

        for (var message : messages) {
            // read message and create job object
            Job job = JsonUtils.deserialize(message.body(), Job.class);

            // check if job was duplicated and already handled
            if (jobIdToClientRequestId.containsKey(job.jobId())) {

                // job was not duplicated, handle normally

                if (job.action() != Job.Action.DONE) {
                    throw new IllegalStateException("Received job with action " + job.action() + " from worker");
                }

                // get client request
                int clientRequestId = jobIdToClientRequestId.get(job.jobId());
                ClientRequest clientRequest = clientRequestIdToClientRequest.get(clientRequestId);

                // add job output to client request and decrement the number of jobs left
                // in the client request
                TitleReviews tr = JsonUtils.deserialize(job.data(), TitleReviews.class);
                clientRequest.addTitleReviews(tr);
                clientRequest.decrementNumJobs();

                log("Received completed job: %s (from client %s request %s)".formatted(job.jobId(), clientRequest.clientId(), clientRequest.requestId()));

                jobIdToClientRequestId.remove(job.jobId()); // remove job id from map

                // check if client request is done
                if (clientRequest.isDone()) {

                    // upload client request output to s3
                    String outputJson = clientRequest.getProcessedReviewsAsJsons();
                    String uploadedName =  clientRequest.fileName() + "_completed";
                    uploadToS3(uploadedName, outputJson);

                    // send completed notification to user
                    CompletedClientRequest completedReq = clientRequest.getCompletedRequest(uploadedName);
                    sendToQueue(USER_OUTPUT_QUEUE_NAME, JsonUtils.serialize(completedReq));

                    // mark client request as done
                    clientRequestIdToClientRequest.remove(clientRequestId);

                    // decrement required workers
                    addToAtomicInteger(-clientRequest.requiredWorkers());

                    log("Completed client request: %s".formatted(clientRequest));

                }
            } else {
                // job was duplicated, ignore
                log("Received duplicate job: %s".formatted(job.jobId()));
            }
        }
    }

    private static void balanceInstanceCount(Box<Exception> exceptionHandler) {

        if(noEc2) return;

        int runningWorkersCount = getWorkerCount(InstanceStateName.RUNNING, InstanceStateName.PENDING);
        int requiredInstanceCount = (int)min(requiredWorkers.get(),(int)Math.ceil(jobIdToClientRequestId.size() / 2.0),MAX_WORKERS);
        int delta = Math.abs(runningWorkersCount - requiredInstanceCount);

        // if the number of running workers is greater than the required number, stop workers
        if(runningWorkersCount > requiredInstanceCount){
            executeLater(()->{
                stopWorkers(delta);
                while(getWorkerCount(InstanceStateName.RUNNING) != requiredInstanceCount){
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {}
                }
                workerCountLock.release();
            },exceptionHandler);
            return;
        }
        // make sure that the number of workers in all states is between 0 and MAX_WORKERS
         else if(runningWorkersCount < requiredInstanceCount){
            startWorkers(delta);
         }
        workerCountLock.release();
    }

    // ============================================================================ |
    // ========================  AWS API FUNCTIONS  =============================== |
    // ============================================================================ |

    private static void createQueuesIfNotExists() {

        boolean queuesCreated = false;

        if(createQueueIfNotExists(WORKER_IN_QUEUE_NAME,WORKER_IN_QUEUE_VISIBILITY_TIMEOUT)) {
            log("Created queue: %s".formatted(WORKER_IN_QUEUE_NAME));
            queuesCreated = true;
        }
        if(createQueueIfNotExists(WORKER_OUT_QUEUE_NAME)) {
            log("Created queue: %s".formatted(WORKER_OUT_QUEUE_NAME));
            queuesCreated = true;
        }
        if(createQueueIfNotExists(WORKER_MANAGEMENT_QUEUE_NAME)) {
            log("Created queue: %s".formatted(WORKER_MANAGEMENT_QUEUE_NAME));
            queuesCreated = true;
        }
        if(createQueueIfNotExists(USER_INPUT_QUEUE_NAME)) {
            log("Created queue: %s".formatted(USER_INPUT_QUEUE_NAME));
            queuesCreated = true;
        }
        if(createQueueIfNotExists(USER_OUTPUT_QUEUE_NAME, 0)) {
            log("Created queue: %s".formatted(USER_OUTPUT_QUEUE_NAME));
            queuesCreated = true;
        }

        if(queuesCreated){
            log("Waiting for queues creation");
            waitForQueuesCreation();
            log("Queues ready");
        }
    }

    private static void waitForQueuesCreation() {
        boolean queuesReady;
        do{
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {}
            queuesReady = new HashSet<>(sqs.listQueues().queueUrls())
                    .containsAll(List.of(
                            getQueueURL(WORKER_IN_QUEUE_NAME),
                            getQueueURL(WORKER_OUT_QUEUE_NAME),
                            getQueueURL(WORKER_MANAGEMENT_QUEUE_NAME),
                            getQueueURL(USER_INPUT_QUEUE_NAME),
                            getQueueURL(USER_OUTPUT_QUEUE_NAME)));

        } while(! queuesReady);
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
        s3.putObject(PutObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key("files/"+key).build(),
                RequestBody.fromString(content));
    }
    private static void appendToS3(String key, String content) {
        String oldContent = "";
        try{
            oldContent = downloadFromS3(key);
        } catch (NoSuchKeyException ignored){}
        uploadToS3(key, oldContent + content);
    }


    private static void stopWorkers(int count) {
        for(int i = 0; i < count; i++){
            Job stopJob = new Job(-1, Job.Action.SHUTDOWN, "");
            sendToQueue(WORKER_MANAGEMENT_QUEUE_NAME, JsonUtils.serialize(stopJob), WORKER_MESSAGE_GROUP_ID);
        }
        if(count > 0) log("Stopped %d workers".formatted(count));
    }

    private static void startWorkers(int count) {
        for (int i = 0; i < count; i++) {
            startWorker(instanceIdCounter++);
        }
        if(count >0) log("Started %d workers".formatted(count));
    }

    private static void startWorker(int id) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(WORKER_IMAGE_ID)
                .tagSpecifications(TagSpecification.builder()
                        .resourceType(ResourceType.INSTANCE)
                        .tags(Tag.builder().key("Name").value("WorkerInstance-"+id).build())
                        .build())
                .securityGroupIds(SECURITY_GROUP)
                .instanceType(WORKER_INSTANCE_TYPE)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .arn("arn:aws:iam::057325794177:instance-profile/LabInstanceProfile")
                        .build())
                .instanceInitiatedShutdownBehavior(ShutdownBehavior.TERMINATE)
                .maxCount(1)
                .minCount(1)
                .userData(Base64.getEncoder().encodeToString(getUserDataScript().getBytes()))
                .build();
        ec2.runInstances(runRequest);
    }

    private static String getUserDataScript() {

        String debugFlags = "";
        if(debugMode){
            debugFlags = "-d";
            if(uploadLogs){
                debugFlags += " -ul worker-%d.log -ui %d".formatted(instanceIdCounter, appendLogIntervalInSeconds);
            }
        }

        return """
                #!/bin/bash
                cd /runtimedir
                java -Xmx7000m -jar workerProgram.jar -workerId %d -inQueueUrl %s -outQueueUrl %s -managerQueueUrl %s -s3BucketName %s -timeout %d %s > output.log 2>&1
                sudo shutdown -h now""".formatted(
                instanceIdCounter,
                getQueueURL(WORKER_IN_QUEUE_NAME),
                getQueueURL(WORKER_OUT_QUEUE_NAME),
                getQueueURL(WORKER_MANAGEMENT_QUEUE_NAME),
                BUCKET_NAME,
                WORKER_IN_QUEUE_VISIBILITY_TIMEOUT,
                debugFlags
        );
    }

    private static void createBucketIfNotExists(String bucketName){
        boolean bucketExists = ! s3.listBuckets().buckets().stream()
                .filter(bucket -> bucket.name().equals(bucketName))
                .toList().isEmpty();

        if(! bucketExists){
            // Create the bucket
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());

            // Wait until the bucket exists
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());

            // create folders
            s3.putObject(PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key("files/")
                            .build(),
                    RequestBody.empty());
            s3.putObject(PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key("files/errors/")
                            .build(),
                    RequestBody.empty());
            s3.putObject(PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key("files/logs/")
                            .build(),
                    RequestBody.empty());
        }
    }

    private static boolean createQueueIfNotExists(String queueName){
        return createQueueIfNotExists(queueName, null);
    }

    private static boolean createQueueIfNotExists(String queueName, Integer visibilityTimeout){

        boolean queueExists = sqs.listQueues(ListQueuesRequest.builder()
                .queueNamePrefix(queueName)
                .build()).hasQueueUrls();

        if(! queueExists){
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .attributes(new HashMap<>(){{
                        if(visibilityTimeout != null){
                            put(QueueAttributeName.VISIBILITY_TIMEOUT, String.valueOf(visibilityTimeout));
                        }
                        if (queueName.endsWith(".fifo")) {
                            put(QueueAttributeName.FIFO_QUEUE, "true");
                            put(QueueAttributeName.CONTENT_BASED_DEDUPLICATION, "false");
                        }
                    }})
                    .build();

            sqs.createQueue(createQueueRequest);
            return true;
        }
        return false;
    }
    private static void deleteBatchFromQueue(String queueName, List<Message> messages){

        List<List<Message>> batches = splitDeleteMessagesToBatches(messages);
        for(List<Message> b : batches){
            sqs.deleteMessageBatch(DeleteMessageBatchRequest.builder()
                    .queueUrl(getQueueURL(queueName))
                    .entries(b.stream()
                            .map(message -> DeleteMessageBatchRequestEntry.builder()
                                    .id(UUID.randomUUID().toString())
                                    .receiptHandle(message.receiptHandle())
                                    .build())
                            .toList())
                    .build());
        }
    }

    private static void sendBatchToQueue(String queueName, List<String> messageBodies) {

        List<List<String>> batches = splitSendMessagesToBatches(messageBodies);
        for(List<String> b : batches){
            sqs.sendMessageBatch(SendMessageBatchRequest.builder()
                    .queueUrl(getQueueURL(queueName))
                    .entries(b.stream()
                            .map(messageBody -> SendMessageBatchRequestEntry.builder()
                                    .id(UUID.randomUUID().toString())
                                    .messageBody(messageBody)
                                    .build())
                            .toList())
                    .build());
        }
    }

    private static List<List<String>> splitSendMessagesToBatches(List<String> messageBodies) {
        List<List<String>> batches = new LinkedList<>();

        List<String> currentBatch = new LinkedList<>();
        int currentBatchBytes = 0;
        for(String message : messageBodies){

            if(currentBatch.size() + 1 == BATCH_REQUEST_MAX_ENTRIES
                    || currentBatchBytes + message.getBytes().length > BATCH_REQUEST_MAX_BYTES){

                batches.add(currentBatch);
                currentBatch = new LinkedList<>();
                currentBatchBytes = 0;
            }
            currentBatch.add(message);
            currentBatchBytes += message.getBytes().length;
        }
        if(! currentBatch.isEmpty()){
            batches.add(currentBatch);
        }

        return batches;
    }

    private static List<List<Message>> splitDeleteMessagesToBatches(List<Message> messageBodies) {
        List<List<Message>> batches = new LinkedList<>();

        List<Message> currentBatch = new LinkedList<>();
        int currentBatchBytes = 0;
        for(Message message : messageBodies){

            if(currentBatch.size() + 1 == BATCH_REQUEST_MAX_ENTRIES
                || currentBatchBytes + message.body().getBytes().length > BATCH_REQUEST_MAX_BYTES){

                batches.add(currentBatch);
                currentBatch = new LinkedList<>();
                currentBatchBytes = 0;
            }
            currentBatch.add(message);
            currentBatchBytes += message.body().getBytes().length;
        }
        if(! currentBatch.isEmpty()){
            batches.add(currentBatch);
        }

        return batches;
    }

    private static void sendToQueue(String queueName, String messageBody) {
        sendToQueue(queueName, messageBody, null);
    }

    private static void sendToQueue(String queueName, String messageBody, String messageGroupId) {
        SendMessageRequest.Builder builder = SendMessageRequest.builder()
                .queueUrl(getQueueURL(queueName))
                .messageBody(messageBody);

        if(messageGroupId != null){
            builder
                    .messageDeduplicationId(getDeDupeId())
                    .messageGroupId(messageGroupId);
        }
        sqs.sendMessage(builder.build());
    }

    private static void deleteFromQueue(Message message, String queueName) {
        sqs.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(getQueueURL(queueName))
                .receiptHandle(message.receiptHandle())
                .build());
    }

    private static int getWorkerCount(InstanceStateName... states) {
        return (int) ec2.describeInstances().reservations().stream()
                .flatMap(reservation -> reservation.instances().stream())
                .filter(instance -> Arrays.asList(states).contains(instance.state().name()))
                .filter(instance -> instance.tags().stream()
                        .anyMatch(tag -> tag.key().equals("Name") && tag.value().startsWith("WorkerInstance")))
                .count();
    }

    // ============================================================================ |
    // ========================  UTILITY FUNCTIONS  =============================== |
    // ============================================================================ |
    private static List<TitleReviews> splitTitleReviews(TitleReviews tr, int splitSize) {

        List<TitleReviews> smallTitleReviewsList = new LinkedList<>(); //will hold all title Reviews with 5 reviews
        List<Review> tempList = new LinkedList<>();
        for (Review rev : tr.reviews()) {

            // if a review is too long, make it a job by itself
            if(rev.text().length() >= BIG_REVIEW_LENGTH){
                smallTitleReviewsList.add(new TitleReviews(tr.title(), List.of(rev)));
                continue;
            }

            tempList.add(rev);
            if (tempList.size() == splitSize) { //counterReview == splitSize
                smallTitleReviewsList.add(new TitleReviews(tr.title(), tempList));
                tempList = new LinkedList<>();
            }
        }
        if(! tempList.isEmpty()){
            smallTitleReviewsList.add(new TitleReviews(tr.title(), tempList));
        }
        return smallTitleReviewsList;
    }

    private static String getQueueURL(String queueName){
        return SQS_DOMAIN_PREFIX + queueName;
    }
    private static String getDeDupeId(){
        return "%s-%s-%s".formatted(MANAGER_ID,Thread.currentThread().getName(),UUID.randomUUID());
    }

    private static void handleException(Exception e) {
        LocalDateTime now = LocalDateTime.now();

        if(e instanceof TerminateException){

            executor.shutdown();
            try {
                while(! executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)){}
            } catch (InterruptedException ignored) {}

            do{
                if(workerCountLock.tryAcquire()){
                    stopWorkers(getWorkerCount(InstanceStateName.RUNNING));
                }
            } while(getWorkerCount(InstanceStateName.RUNNING) != 0);

            waitUntilAllWorkersStopped();
            if(uploadLogs && ! uploadBuffer.isEmpty()){
                appendToS3("logs/"+uploadLogName, uploadBuffer.toString());
            }
            System.exit(0);
        }

        String timeStamp = getTimeStamp(now);
        String logName = "errors/%s error_manager.log".formatted(timeStamp);

        String stackTrace = stackTraceToString(e);
        uploadToS3(logName,stackTrace);
        log("Exception in thread %s:\n%s".formatted(Thread.currentThread(),stackTraceToString(e)));
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

    private static void waitUntilAllWorkersStopped() {
        while(true){
            if (getWorkerCount(InstanceStateName.RUNNING) != 0){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {}
            } else {
                return;
            }
        }
    }

    public static int addTitleReviews(TitleReviews tr, List<TitleReviews> list){
        int reviewsCount = tr.reviews().size();
        for(TitleReviews o : list){
            if(o.title().equals(tr.title())){
                o.reviews().addAll(tr.reviews());
                return reviewsCount;
            }
        }
        list.add(tr);
        return reviewsCount;
    }

    private static String stackTraceToString(Exception e) {
        StringBuilder output  = new StringBuilder();
        output.append(e).append("\n");
        for (var element: e.getStackTrace()) {
            output.append("\t").append(element).append("\n");
        }
        return output.toString();
    }

    private static void printUsageAndExit(String errorMessage) {
        if(! errorMessage.equals("")) {
            System.out.println(errorMessage);
        }
        System.out.println(USAGE);
        System.exit(1);
    }

    private static void addToAtomicInteger(int num) {
        int currentRequiredWorkers;
        int newRequiredWorkers;
        do{
            currentRequiredWorkers = requiredWorkers.get();
            newRequiredWorkers = currentRequiredWorkers + num;
        } while (! requiredWorkers.compareAndSet(currentRequiredWorkers, newRequiredWorkers));
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

    private static long min(long... nums){
        long min = Long.MAX_VALUE;
        for (long num : nums) {
            if(num < min){
                min = num;
            }
        }
        return min;
    }

    private static void executeLater(Runnable r,Box<Exception> exceptionHandler){
        executor.execute(()->{
            try{
                r.run();
            } catch (Exception e){
                exceptionHandler.set(e);
            }
        });

    }

    private static void readArgs(String[] args) {

        List<String> helpOptions = List.of("-h","-help");
        List<String> debugModeOptions = List.of("-d","-debug");
        List<String> uploadLogOptions = List.of("-ul","-uploadlog");
        List<String> uploadIntervalOptions = List.of("-ui","-uploadinterval");
        List<String> argsList = new LinkedList<>();
        argsList.addAll(helpOptions);
        argsList.addAll(debugModeOptions);
        argsList.addAll(uploadLogOptions);
        argsList.addAll(uploadIntervalOptions);
        argsList.add("-noec2");

        for (int i = 0; i < args.length; i++) {
            String arg = args[i].toLowerCase();
            String errorMessage;

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
            if(arg.equals("-noec2")){
                noEc2 = true;
                continue;
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
    }
}



