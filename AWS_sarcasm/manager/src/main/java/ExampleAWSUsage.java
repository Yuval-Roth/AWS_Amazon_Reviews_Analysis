
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.Base64;
import java.util.HashMap;
import java.util.Random;


public class ExampleAWSUsage {

    public static final String BUCKET_NAME = "distributed-systems-2024-bucket-yuval-adi";
    public static final String IMAGE_ID = "ami-0f3fcfc1ba98c6fb9";
    public static final String SECURITY_GROUP = "sg-00c67312e0a74a525";
    public static final String INSTANCE_TYPE = "t2.medium";
    private static final String SQS_DOMAIN_PREFIX = "https://sqs.us-east-1.amazonaws.com/057325794177/";
    private static S3Client s3;
    private static Ec2Client ec2;
    private static SqsClient sqs;

    private static Region ec2_region = Region.US_EAST_1;
    private static Region s3_region = Region.US_WEST_2;
    private static volatile boolean shouldRun;


    public static void main(String[] args) {
        ec2 = Ec2Client.builder()
                .region(ec2_region)
                .build();
        s3 = S3Client.builder()
                .region(s3_region)
                .build();
        sqs = SqsClient.builder()
                .region(ec2_region)
                .build();

//        createPublicBucketIfNotExists(BUCKET_NAME);


        String queueName = "jobsQueue";
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName + ".fifo")
                .attributes(new HashMap<>(){{
                    put(QueueAttributeName.FIFO_QUEUE, "true");
                    put(QueueAttributeName.CONTENT_BASED_DEDUPLICATION, "false");
                }})
                .build();

//        sqs.createQueue(createQueueRequest);

        for (int i = 0; i < 15; i++) {
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(SQS_DOMAIN_PREFIX  + queueName + ".fifo")
                    .messageBody( "This is a test message number "+i)
                    .messageGroupId("1")
                    .messageDeduplicationId(String.valueOf(new Random().nextInt()))
                    .build());
        }


        //Example of sending and receiving messages from the queue
        Thread t1 = new Thread(() -> tMain(queueName));
        Thread t2 = new Thread(() -> tMain(queueName));

        shouldRun = true;

        t1.start();
        t2.start();


        GetQueueAttributesRequest getQueueAttributesRequest = GetQueueAttributesRequest.builder()
                .queueUrl(SQS_DOMAIN_PREFIX + queueName + ".fifo")
                .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                .build();

        int approximateNumberOfMessages;

        do {

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            GetQueueAttributesResponse getQueueAttributesResponse = sqs.getQueueAttributes(getQueueAttributesRequest);
            approximateNumberOfMessages = Integer.parseInt(
                    getQueueAttributesResponse.attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
            );

            System.out.println("Approximate number of messages: " + approximateNumberOfMessages);

        } while (approximateNumberOfMessages > 0);

        shouldRun = false;
        try {
            t1.join();
            t2.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(IMAGE_ID)
                .tagSpecifications(TagSpecification.builder()
                        .resourceType(ResourceType.INSTANCE)
                        .tags(Tag.builder().key("Name").value("WorkerInstance").build())
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

        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder()
                .filters(Filter.builder()
                        .name("tag:Name")
                        .values("WorkerInstance")
                        .build(),
                        Filter.builder()
                                .name("instance-state-name")
                                .values("running", "pending")
                                .build())
                .build();

        var r = ec2.describeInstances(describeInstancesRequest);
        for (var reservation : r.reservations()) {
            for (var instance : reservation.instances()) {
                System.out.println(instance.instanceId());
            }
        }

    }

    private static void tMain(String queueName) {
        ReceiveMessageRequest messageRequest = ReceiveMessageRequest.builder()
                .maxNumberOfMessages(1)
                .queueUrl(SQS_DOMAIN_PREFIX + queueName + ".fifo")
                .build();

        while(shouldRun) {
            var r =  sqs.receiveMessage(messageRequest);
            if(r.hasMessages()){

                System.out.println(Thread.currentThread().getName()+": "+r.messages().getFirst().body());

                sqs.deleteMessage(software.amazon.awssdk.services.sqs.model.DeleteMessageRequest.builder()
                        .queueUrl(SQS_DOMAIN_PREFIX + queueName + ".fifo")
                        .receiptHandle(r.messages().getFirst().receiptHandle())
                        .build());
            } else {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static String getUserDataScript() {

        String workerJarUri = "s3://" + BUCKET_NAME + "/workerMain.jar";
        String input1Uri = "s3://" + BUCKET_NAME + "/input1.txt";
        String outputUri = "s3://" + BUCKET_NAME + "/output.log";

        return """
                #!/bin/bash
                mkdir /runtimedir
                cd /runtimedir
                aws s3 cp %s input1.txt > input1_download.log 2>&1
                aws s3 cp %s workerMain.jar > workerMain_download.log 2>&1
                java -Xmx3500m -Xms3000m -jar workerMain.jar input1.txt > output.log 2>&1
                aws s3 cp output.log %s > output_upload.log 2>&1
                sudo shutdown -h now""".formatted(input1Uri,workerJarUri,outputUri);

    }

    public static void createPublicBucketIfNotExists(String bucketName) {
        try {
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

        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

}
