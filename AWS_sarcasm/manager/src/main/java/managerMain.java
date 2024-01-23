
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.util.Base64;


public class managerMain {

    public static final String BUCKET_NAME = "distributed-systems-2024-bucket-yuval-adi";
    public static String WORKER_JAR_URL;
    public static String INPUT1_URL;
    private static S3Client s3;
    private static Ec2Client ec2;
    private static SqsClient sqs;

    private static Region region = Region.US_EAST_1;


    public static void main(String[] args) {
        ec2 = Ec2Client.builder()
                .region(region)
                .build();
        s3 = S3Client.builder()
                .region(region)
                .build();
        sqs = SqsClient.builder()
                .region(region)
                .build();

//        createBucketIfNotExists(BUCKET_NAME);

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId("ami-0142bda45f1329b54")
                .securityGroupIds("sg-086e72012ac49678a")
                .instanceType("t2.large")
                .maxCount(1)
                .minCount(1)
                .userData(Base64.getEncoder().encodeToString(getUserDataScript().getBytes()))
                .build();
        ec2.runInstances(runRequest);
    }

    private static String getUserDataScript() {

        WORKER_JAR_URL = s3.utilities().getUrl(GetUrlRequest.builder()
                .bucket(BUCKET_NAME)
                .key("workerMain.jar")
                .build()).toString();

        INPUT1_URL = s3.utilities().getUrl(GetUrlRequest.builder()
                .bucket(BUCKET_NAME)
                .key("input1.txt")
                .build()).toString();

        return """
                #!/bin/bash
                cd /
                wget -O workerMain.jar %s
                wget -O input1.txt %s
                java -jar workerMain.jar input1.txt > output.log 2>&1""".formatted(WORKER_JAR_URL, INPUT1_URL);
    }

    public static void createBucketIfNotExists(String bucketName) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
//                    .acl(BucketCannedACL.PUBLIC_READ)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

}
