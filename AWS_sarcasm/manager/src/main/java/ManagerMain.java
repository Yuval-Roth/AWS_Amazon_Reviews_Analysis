
import edu.stanford.nlp.semgraph.SemanticGraph;
import org.apache.commons.lang3.NotImplementedException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.*;
import java.util.concurrent.Semaphore;

public class ManagerMain {

    // <S3>
    public static final String BUCKET_NAME = "distributed-systems-2024-bucket-yuval-adi";
    private static final int TR_JOB_SPLIT_SIZE = 5;
    private static S3Client s3;
    // </S3>

    // <EC2>
    public static final String WORKER_IMAGE_ID = "ami-0f3fcfc1ba98c6fb9";
    public static final String SECURITY_GROUP = "sg-00c67312e0a74a525";
    public static final String INSTANCE_TYPE = "t2.medium";
    public static final int MANAGER_ID = 1;
    private static Ec2Client ec2;
    private static int workerCount;
    private static int instanceIdCounter;
    // </EC2>


    // <SQS>
    private static final String WORKER_MESSAGE_GROUP_ID = "workerGroup";
    private static final String WORKER_IN_QUEUE_NAME = "workerInQueue";
    private static final String WORKER_OUT_QUEUE_NAME = "workerOutQueue";
    private static final String WORKER_MANAGEMENT_QUEUE_NAME = "workerManagementQueue";
    private static final String USER_MESSAGE_GROUP_ID = "userGroup";
    private static final String USER_INPUT_QUEUE_NAME = "userInputQueue";
    private static final String USER_OUTPUT_QUEUE_NAME = "userOutputQueue";
    private static final String SQS_DOMAIN_PREFIX = "https://sqs.us-east-1.amazonaws.com/057325794177/";
    private static final int MAX_WORKERS = 18;
    private static SqsClient sqs;
    // </SQS>

    private static final Region ec2_region = Region.US_EAST_1;
    private static final Region s3_region = Region.US_WEST_2;
    private static int jobIdCounter;
    private static Map<Integer,ClientRequest> clientRequestIdToClientRequest;
    private static Map<Integer, Integer> jobIdToClientRequestId;
    private static Semaphore completedJobsLock;
    private static Semaphore clientRequestsLock;
    private static Semaphore workerCountLock;
    private static boolean debugMode;


    public static void main(String[] args) {

        if(args.length > 0 && args[0].equals("debug")){
            debugMode = true;
        }

        sqs = SqsClient.builder()
                .region(ec2_region)
                .build();

        s3 = S3Client.builder()
                .region(s3_region)
                .build();

        ec2 = Ec2Client.builder()
                .region(ec2_region)
                .build();

        jobIdToClientRequestId = new HashMap<>();
        clientRequestIdToClientRequest = new HashMap<>();
        jobIdCounter = 0;
        instanceIdCounter = 2;
        completedJobsLock = new Semaphore(1);
        clientRequestsLock = new Semaphore(1);
        workerCountLock = new Semaphore(1);

        createBucketIfNotExists(BUCKET_NAME);
        createQueueIfNotExists(WORKER_IN_QUEUE_NAME);
        createQueueIfNotExists(WORKER_OUT_QUEUE_NAME);
        createQueueIfNotExists(WORKER_MANAGEMENT_QUEUE_NAME);
        createQueueIfNotExists(USER_INPUT_QUEUE_NAME);
        createQueueIfNotExists(USER_OUTPUT_QUEUE_NAME);


        final Exception[] exceptionHandler = new Exception[1];

        while(true){
            Thread t = new Thread(()-> mainLoop(exceptionHandler));
            t.start();
            mainLoop(exceptionHandler);

            try {
                t.join();
            } catch (InterruptedException ignored) {}
            if(exceptionHandler[0] != null){
                handleException(exceptionHandler[0]);
                exceptionHandler[0] = null;
            }
        }

    }

    private static void mainLoop(Exception[] exceptionHandler) {

        while(exceptionHandler[0] == null){
            try{

                if(clientRequestsLock.tryAcquire()) {
                    checkForClientRequests();
                }

                if(completedJobsLock.tryAcquire()) {
                    checkForCompletedJobs();
                }

                if(! debugMode){
                    if(workerCountLock.tryAcquire()) {
                        balanceWorkerCount();
                    }
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {}


            } catch (Exception e){
                exceptionHandler[0] = e;
                return;
            }
        }
    }

    private static void balanceWorkerCount() {

        // get number of workers
        int workerCount = getWorkerCount();
        int requiredWorkerCount = Math.min(MAX_WORKERS,getRequiredWorkerCount());

        if(workerCount < requiredWorkerCount){
            startWorkers(requiredWorkerCount - workerCount);
        } else if(workerCount > requiredWorkerCount){
            stopWorkers(workerCount - requiredWorkerCount);
        }
    }

    private static void stopWorkers(int count) {
        for(int i = 0; i < count; i++){
            Job stopJob = new Job(-1, Job.Action.SHUTDOWN, "");
            sendToQueue(WORKER_MANAGEMENT_QUEUE_NAME, JsonUtils.serialize(stopJob), WORKER_MESSAGE_GROUP_ID);
        }
    }

    private static int getRequiredWorkerCount() {
        //TODO: implement
        throw new NotImplementedException("getRequiredWorkerCount not implemented");
    }

    private static int getWorkerCount() {
        return (int) ec2.describeInstances().reservations().stream()
                .flatMap(reservation -> reservation.instances().stream())
                .filter(instance -> ! instance.state().name().equals(InstanceStateName.TERMINATED))
                .filter(instance -> instance.tags().stream()
                        .noneMatch(tag -> tag.key().equals("Name") && tag.value().equals("ManagerInstance")))
                .count();
    }

    private static void handleException(Exception e) {

        //TODO: make a more robust exception handling
        e.printStackTrace();

    }

    private static void checkForCompletedJobs(){

        List<SendMessageBatchRequestEntry> messagesToSend = new LinkedList<>();

        ReceiveMessageRequest messageRequest = ReceiveMessageRequest.builder()
                .queueUrl(getQueueURL(WORKER_OUT_QUEUE_NAME))
                .build();

        var r = sqs.receiveMessage(messageRequest);

        if(r.hasMessages()){
            for(var message : r.messages()) {
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
                    jobIdToClientRequestId.remove(job.jobId()); // remove job id from map

                    // check if client request is done
                    if (clientRequest.isDone()) {

                        // we want to delete the messages from the queue as soon as possible,
                        // so we send the finished requests later
                        CompletedClientRequest completedClientRequest = clientRequest.getCompletedRequest();
                        String messageBody = JsonUtils.serialize(completedClientRequest);
                        messagesToSend.add(SendMessageBatchRequestEntry.builder()
                                        .messageBody(messageBody)
                                        .messageDeduplicationId(getDeDupeId())
                                        .messageGroupId(clientRequest.clientId())
                                .build());

                        // remove client request
                        clientRequestIdToClientRequest.remove(clientRequestId);
                    }
                }

                // delete message from queue
                sqs.deleteMessage(software.amazon.awssdk.services.sqs.model.DeleteMessageRequest.builder()
                        .queueUrl(getQueueURL(WORKER_OUT_QUEUE_NAME))
                        .receiptHandle(message.receiptHandle())
                        .build());
            }

            // send completed client requests to users
            if(! messagesToSend.isEmpty()){
                sqs.sendMessageBatch(SendMessageBatchRequest.builder()
                        .queueUrl(getQueueURL(USER_OUTPUT_QUEUE_NAME))
                        .entries(messagesToSend)
                        .build());
            }
        }
    }

    private static void checkForClientRequests() {

        ReceiveMessageRequest messageRequest = ReceiveMessageRequest.builder()
                .maxNumberOfMessages(1)
                .queueUrl(getQueueURL(USER_INPUT_QUEUE_NAME))
                .build();

        var r = sqs.receiveMessage(messageRequest);
        
        if(r.hasMessages()){

            // read message and create client request
            String message = r.messages().getFirst().body();
            ClientRequest clientRequest = JsonUtils.deserialize(message, ClientRequest.class);
            clientRequestIdToClientRequest.put(clientRequest.requestId(), clientRequest);

            // Deserialize client request input and split to small TitleReviews
            String[] jsons = clientRequest.input().split("\n");
            TitleReviews[] titleReviewsList = Arrays.stream(jsons)
  /*deserialize*/   .map(json -> JsonUtils.<TitleReviews>deserialize(json, TitleReviews.class))
        /*split*/   .flatMap(tr -> splitTitleReviews(tr, TR_JOB_SPLIT_SIZE).stream())
                    .toArray(TitleReviews[]::new);

            // split client request to jobs and send to workers
            for (TitleReviews tr : titleReviewsList) {

                // create job and increment job id
                String jsonJob = JsonUtils.serialize(tr);
                Job job = new Job(jobIdCounter++, Job.Action.PROCESS, jsonJob);
                String messageBody = JsonUtils.serialize(job);

                // map job id to client request id
                jobIdToClientRequestId.put(job.jobId(),clientRequest.requestId());

                // send job to worker
                sendToQueue(WORKER_IN_QUEUE_NAME, messageBody,WORKER_MESSAGE_GROUP_ID);
            }
        }
    }

    private static List<TitleReviews> splitTitleReviews(TitleReviews tr,int splitSize) {
        List<TitleReviews> smallTitleReviewsList = new LinkedList<>(); //will hold all title Reviews with 5 reviews
        int counterReviews = 0;
        List<Review> tempList = new LinkedList<>();
        TitleReviews smallTr = new TitleReviews(tr.title(), tempList);
        for (Review rev : tr.reviews()) {
            if (counterReviews < splitSize) {
                tempList.add(rev);
                counterReviews++;
            } else { //counterReview == splitSize
                smallTitleReviewsList.add(smallTr);
                tempList = new LinkedList<>();
                smallTr = new TitleReviews(tr.title(), tempList);
                counterReviews = 0;
            }
        }
        return smallTitleReviewsList;
    }

    private static void sendToQueue(String queueName, String messageBody, String messageGroupId) {
        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(getQueueURL(queueName))
                .messageBody(messageBody)
                .messageGroupId(messageGroupId)
                .messageDeduplicationId(getDeDupeId())
                .build());
    }

    private static String getDeDupeId(){
        return "%s-%s-%s".formatted(MANAGER_ID,Thread.currentThread().getName(),UUID.randomUUID());
    }

    private static void startWorkers(int count) {
        for (int i = 0; i < count; i++) {
            startWorker(instanceIdCounter++);
        }
    }

    private static void startWorker(int id) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(WORKER_IMAGE_ID)
                .tagSpecifications(TagSpecification.builder()
                        .resourceType(ResourceType.INSTANCE)
                        .tags(Tag.builder().key("Name").value("WorkerInstance-"+id).build())
                        .build())
                .securityGroupIds(SECURITY_GROUP)
                .instanceType(INSTANCE_TYPE)
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

        String workerJarUri = "s3://" + BUCKET_NAME + "/workerMain.jar";

        return """
                #!/bin/bash
                mkdir /runtimedir
                cd /runtimedir
                aws s3 cp %s workerMain.jar > workerMain_download.log 2>&1
                java -Xmx4500m -Xms3750m -jar workerMain.jar %s %s %s %s %s > output.log 2>&1
                sudo shutdown -h now""".formatted(
                        workerJarUri,
                        getQueueURL(getQueueURL(WORKER_IN_QUEUE_NAME)),
                        getQueueURL(getQueueURL(WORKER_OUT_QUEUE_NAME)),
                        getQueueURL(getQueueURL(WORKER_MANAGEMENT_QUEUE_NAME)),
                        WORKER_MESSAGE_GROUP_ID,
                        instanceIdCounter
                );

    }

    private static String getQueueURL(String queueName){
        return SQS_DOMAIN_PREFIX + queueName + ".fifo";
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
        }
    }

    private static void createQueueIfNotExists(String queueName){

        boolean queueExists = sqs.listQueues(ListQueuesRequest.builder()
                        .queueNamePrefix(queueName)
                        .build()).hasQueueUrls();

        if(! queueExists){
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName + ".fifo")
                    .attributes(new HashMap<>(){{
                        put(QueueAttributeName.FIFO_QUEUE, "true");
                        put(QueueAttributeName.CONTENT_BASED_DEDUPLICATION, "false");
                    }})
                    .build();

            sqs.createQueue(createQueueRequest);
        }
    }
}



