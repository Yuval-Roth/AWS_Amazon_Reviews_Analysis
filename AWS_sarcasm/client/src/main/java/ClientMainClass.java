import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.protocols.jsoncore.JsonWriter;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.*;
import java.util.*;

public class ClientMainClass {

    // <S3>
    public static final String BUCKET_NAME = "distributed-systems-2024-bucket-yuval-adi";
    public static final int BIG_REVIEW_LENGTH = 800;
    private static S3Client s3;
    // </S3>

    // <EC2>
    public static final String MANAGER_IMAGE_ID = "";
    public static final String WORKER_IMAGE_ID = "";
    public static final String SECURITY_GROUP = "sg-00c67312e0a74a525";
    public static final String MANAGER_INSTANCE_TYPE = "t3.micro";
    public static final int MANAGER_ID = 1;
    private static Ec2Client ec2;
    private static int instanceIdCounter;
    // </EC2>

    // <SQS>
    private static final String USER_INPUT_QUEUE_NAME = "userInputQueue";
    private static final String USER_OUTPUT_QUEUE_NAME = "userOutputQueue";
    private static final String SQS_DOMAIN_PREFIX = "https://sqs.us-east-1.amazonaws.com/057325794177/";
    private static SqsClient sqs;
    // </SQS>

    private static final Region ec2_region = Region.US_EAST_1;
    private static final Region s3_region = Region.US_WEST_2;

    private static String clientId;
    private static int requestId;
    private static boolean debugMode;
    private static boolean uploadLogs;

    private static int appendLogIntervalInSeconds;
    private static boolean noEc2;
    private static Scanner scanner;

    private static Map<Integer,ClientRequest> clientRequestMap;
    private static Map<Integer,Status> clientRequestsStatusMap;

    enum Status {
        DONE,
        IN_PROGRESS,
    }
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

        //startManagerIfNotExists();
        requestId = 0;
        clientId = UUID.randomUUID().toString();
        clientRequestMap = new HashMap<>();
        clientRequestsStatusMap = new HashMap<>();

        scanner = new Scanner(System.in);
        while(true){
            System.out.println("Choose an option:");
            System.out.println("1. Send new request");
            System.out.println("2. Show requests");
            System.out.println("3. Open finished request");
            int choice = scanner.nextInt();
            switch (choice){
                case 1-> sendNewRequest();
                case 2-> showRequests();
                case 3-> openFinishedRequest();
            }
        }
    }


    private static void openFinishedRequest() {
        System.out.println("Enter request id:");
        int requestId = scanner.nextInt();
    }

    private static void showRequests() {
        System.out.println("Request id  |  File name  |  Status");
        for(var entry: clientRequestMap.entrySet()){
            System.out.printf("%d|%s|%s%n", entry.getKey(), entry.getValue().fileName(), clientRequestsStatusMap.get(entry.getKey()));
        }
    }

    private static void sendNewRequest() {
        System.out.print("File name: ");
        String fileName = scanner.next();
        System.out.print("Reviews per worker: ");
        int reviewsPerWorker = scanner.nextInt();
        System.out.print("Terminate(t/f): ");
        String terminate = scanner.next();
        Boolean terminateB = terminate.equals("t") ? true : terminate.equals("f") ? false : null;
        sendClientRequest(fileName,reviewsPerWorker,terminateB);
    }

    private static void sendClientRequest(String fileName, int reviewsPerWorker, boolean terminate) {
        String input = readInputFile(fileName);
        String path = "temp/%s/%s___%s".formatted(clientId, UUID.randomUUID(), fileName);
        uploadToS3(path, input);
        ClientRequest toSend = new ClientRequest(clientId, requestId, fileName, reviewsPerWorker, terminate);
        clientRequestMap.put(requestId, toSend);
        clientRequestsStatusMap.put(requestId,Status.IN_PROGRESS);
        requestId++;
        sqs.sendMessage(SendMessageRequest.builder()
                      .queueUrl(getQueueURL(USER_INPUT_QUEUE_NAME))
                      .messageBody(JsonUtils.serialize(toSend))
            .build());
    }

    private static void startManagerIfNotExists() {
        var r = ec2.describeInstances(DescribeInstancesRequest.builder()
                .filters(Filter.builder()
                        .name("tag:Name")
                        .values("ManagerInstance")
                        .build())
                .build());
        boolean managerExists = 0 != r.reservations().stream()
                .reduce(0, (acc, res) -> acc + res.instances().size(), Integer::sum);
        if (!managerExists) {
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .imageId(MANAGER_IMAGE_ID)
                    .tagSpecifications(TagSpecification.builder()
                            .resourceType(ResourceType.INSTANCE)
                            .tags(Tag.builder().key("Name").value("ManagerInstance").build())
                            .build())
                    .securityGroupIds(SECURITY_GROUP)
                    .instanceType(MANAGER_INSTANCE_TYPE)
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
    }

    private static String getUserDataScript() {
        String debugFlags = "";
        if(debugMode){
            debugFlags = "-d";
            if(uploadLogs){
                debugFlags += " -ul manager.log -ui %d".formatted(appendLogIntervalInSeconds);
            }
        }
        if(noEc2){
           debugFlags+= " -noEc2";
        }

        return """
                #!/bin/bash
                cd /runtimedir
                java -jar managerProgram.jar -workerImageId %s %s > output.log 2>&1
                sudo shutdown -h now""".formatted(WORKER_IMAGE_ID,
                debugFlags
        );
    }

    private static String getQueueURL(String queueName){
        return SQS_DOMAIN_PREFIX+queueName;
    }

    private static void uploadToS3(String fileName, String input) {
        s3.putObject(PutObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key("files/"+ fileName)
                .build(), RequestBody.fromString(input));
    }

    public static String readInputFile(String fileName){
        String path = getFolderPath()+"/input files/"+fileName;
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        try(BufferedReader buffReader =  new BufferedReader(new FileReader(path))){
            while((line = buffReader.readLine())!=null) {
                stringBuilder.append(line).append("\n");
            }
        }
        catch (FileNotFoundException e){
            System.out.println("File not found");
        }
        catch(IOException e){
            e.printStackTrace();
        }
        return stringBuilder.toString();
    }

    private static String getFolderPath() {
        String folderPath = ClientMainClass.class.getResource("ClientMainClass.class").getPath();
        folderPath = folderPath.replace("%20"," "); //fix space character
        folderPath = folderPath.substring(folderPath.indexOf("/")+1); // remove initial '/'
        folderPath = folderPath.substring(0,folderPath.lastIndexOf("/")); // remove .class file from path
        folderPath = folderPath.substring(0,folderPath.lastIndexOf("/")); // exit jar
        return folderPath;
    }
}
