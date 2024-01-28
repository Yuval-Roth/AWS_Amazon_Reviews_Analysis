
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class ManagerMain {
    public static final String BUCKET_NAME = "distributed-systems-2024-bucket-yuval-adi";
    public static final String WORKER_IMAGE_ID = "ami-0f3fcfc1ba98c6fb9";
    public static final String SECURITY_GROUP = "sg-00c67312e0a74a525";
    public static final String INSTANCE_TYPE = "t2.medium";
    private static final String WORKER_MESSAGE_GROUP_ID = "workerGroup";
    private static final String WORKER_IN_QUEUE_NAME = "workerInQueue";
    private static final String WORKER_OUT_QUEUE_NAME = "workerOutQueue";
    private static final String USER_INPUT_MESSAGE_GROUP_ID = "userInputGroup";
    private static final String USER_INPUT_QUEUE_NAME = "userInputQueue";
    private static final String USER_OUTPUT_QUEUE_NAME = "userOutputQueue";
    private static final String SQS_DOMAIN_PREFIX = "https://sqs.us-east-1.amazonaws.com/057325794177/";
    private static Region ec2_region = Region.US_EAST_1;
    private static Region s3_region = Region.US_WEST_2;
    private static SqsClient sqs;
    private static S3Client s3;
    private static Ec2Client ec2;
    private static int workerCount;
    private static int workerId;
    private static String

    public static void main(String[] args) {

        sqs = SqsClient.builder()
                .region(ec2_region)
                .build();

        s3 = S3Client.builder()
                .region(s3_region)
                .build();

        ec2 = Ec2Client.builder()
                .region(ec2_region)
                .build();


    }

    private static void tempMain() {

        String queueName = "user-input";
        ReceiveMessageRequest messageRequest = ReceiveMessageRequest.builder()
                .maxNumberOfMessages(1)
                .queueUrl(SQS_DOMAIN_PREFIX + queueName + ".fifo")
                .build();

        var r = sqs.receiveMessage(messageRequest);
        String message = r.messages().getFirst().body();
        String[] messages = message.split("\n");
        for (String json : messages) {
            int counterReviews = 0;
            List<TitleReviews> titleReviewsList = new LinkedList<>();
            TitleReviews tr = JsonUtils.deserialize(json, TitleReviews.class);
//            for (:
//                 ){
//
//            }
        }
    }

    private static void startWorkers(int count) {
        for (int i = 0; i < count; i++) {
            startWorker(workerId++);
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
                java -Xmx3500m -Xms3000m -jar workerMain.jar %s %s %s %s > output.log 2>&1
                sudo shutdown -h now""".formatted(
                        workerJarUri,
                        getQueueURL(WORKER_IN_QUEUE_NAME),
                        getQueueURL(WORKER_OUT_QUEUE_NAME),
                        WORKER_MESSAGE_GROUP_ID,
                        workerId
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



