import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.awt.*;
import java.io.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientMainClass {
    enum Status {
        DONE,
        IN_PROGRESS;

        public String toString(){
            return switch(this){
                case DONE -> "Done";
                case IN_PROGRESS -> "In progress";
            };
        }
    }

    // <S3>
    public static final String BUCKET_NAME = "distributed-systems-2024-bucket-yuval-adi";
    private static final Region s3_region = Region.US_WEST_2;
    private static S3Client s3;

    // </S3>
    // <EC2>
    public static String MANAGER_IMAGE_ID;
    public static final String SECURITY_GROUP = "sg-00c67312e0a74a525";
    public static final String MANAGER_INSTANCE_TYPE = "t3.micro";
    private static Ec2Client ec2;
    private static final Region ec2_region = Region.US_EAST_1;

    // </EC2>
    // <SQS>
    private static final String USER_INPUT_QUEUE_NAME = "userInputQueue";
    private static final String USER_OUTPUT_QUEUE_NAME = "userOutputQueue";
    private static final String SQS_DOMAIN_PREFIX = "https://sqs.us-east-1.amazonaws.com/057325794177/";
    private static SqsClient sqs;

    // </SQS>
    // <DEBUG FLAGS>
    private static final String USAGE = """
                Usage: java -jar clientProgram.jar [-h | -help] [-d] [optional debug flags]
                                    
                -h | -help :- Print this message and exit.
                                    
                -d | -debug :- Run in debug mode, logging all operations to standard output
                
                optional debug flags:
                    
                    -ul | -uploadLog :- Ec2 instances will upload their logs to the S3 bucket.
                                  Must be used with -debug.
                                  
                    -ui | -uploadInterval <interval in seconds> :- When combined with -uploadLog, specifies the interval in seconds
                                  between log uploads to the S3 bucket.
                                  Must be a positive integer, must be used with -uploadLog.
                                  If this argument is not specified, defaults to 60 seconds.
                                  
                    -noEc2 :- Run without creating worker instances. Useful for debugging locally.
                    
                    -noManager :- Run without creating manager instance. Useful for debugging locally.
                                  All other debug flags are ignored when this flag is used.
                """;
    private static volatile boolean debugMode;
    private static volatile boolean noEc2;
    private static volatile boolean uploadLogs;
    private static volatile int appendLogIntervalInSeconds;
    private static boolean noManager;

    // </DEBUG FLAGS>

    // <APPLICATION DATA>
    private static String clientId;
    private static int requestId;
    private static final String BASE_HTML_ROW = """
            <tr >
                <td style="background-color: %s; width: 50px; height: 100px"></td>
                <td>
                    <ul style="line-height: 25px; padding-top: 0px; padding-bottom: 0px">
                        <li style="padding-bottom: 5px; padding-top: 5px">Subject: %s</li>
                        <li style="padding-bottom: 5px; padding-top: 5px"><a href="%s">%s</a></li>
                        <li style="padding-bottom: 5px; padding-top: 5px">Entities: %s</li>
                        <li style="padding-bottom: 5px; padding-top: 5px">Sarcasm: %s</li>
                    </ul>
                </td>
            </tr>""";

    private static final String BASE_HTML_DOC = """
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>%s</title>
                <style>
                    body {
                        text-align: center;
                        margin: 20px;
                    }
                    table {
                        border-collapse: collapse;
                        width: 80%;
                        margin: 20px auto;
                    }
                            
                    th, td {
                        border: 1px solid #dddddd;
                        text-align: left;
                    }
                    th {
                        background-color: #f2f2f2;
                    }
                </style>
            </head>
            <body>
            <h2>%s results</h2>
            <table>
                <tbody>
                %s
                </tbody>
            </table>
            </body>
            </html>""";
    private static Map<Integer,ClientRequest> clientRequestMap;
    private static Map<Integer,Status> clientRequestsStatusMap;
    private static AtomicBoolean newFinishedRequest = new AtomicBoolean(false);



    // </APPLICATION DATA>

    public static void main(String[] args) {

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


        if(! noManager){
            var r =  ec2.describeImages(DescribeImagesRequest.builder()
                    .filters(Filter.builder()
                            .name("name")
                            .values("managerImage")
                            .build())
                    .build());
            try{
                MANAGER_IMAGE_ID = r.images().getFirst().imageId();
            } catch(NoSuchElementException e){
                log("No manager image found");
                handleException(new TerminateException());
            }
        }


        requestId = 0;
        clientId = UUID.randomUUID().toString();
        clientRequestMap = new HashMap<>();
        clientRequestsStatusMap = new HashMap<>();

        Box<Exception> exceptionHandler = new Box<>(null);

        while(true) {

            Thread secondaryThread = new Thread(()->secondaryLoop(exceptionHandler) ,"secondary");
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

//        String output = readInputFile("output1.txt");
//        createHtmlFile(output,"output1.txt");
    }

    private static void handleException(Exception exception) {
        exception.printStackTrace();
    }

    private static void mainLoop(Box<Exception> exceptionHandler) {
        while (exceptionHandler.get() == null) {
            try {
                System.out.println("Choose an option:");
                System.out.println("1. Send new request");
                System.out.println("2. Show requests");
                System.out.println("3. Open finished request");
                System.out.println("4. Exit");
                System.out.print(">> ");
                String choice;

                // wait for input or new finished request
                while(System.in.available() == 0){

                    // if there are new finished requests, ask the user if they want to see them
                    if(newFinishedRequest.get()){
                        newFinishedRequest.set(false);
                        System.out.println("\r  \nThere are new finished requests, would you like to see them? (y/n)");
                        System.out.print(">> ");
                        choice = readLine().toLowerCase();
                        if(choice.equals("y") || choice.equals("yes")){
                            showRequests();
                        }
                        break;
                    }

                    Thread.sleep(100);
                }

                // if there is input, read it
                if(System.in.available() > 0){
                    choice = readLine();
                } else {
                    continue;
                }
                switch (choice) {
                    case "1" -> sendNewRequest();
                    case "2" -> showRequests();
                    case "3" -> openFinishedRequest();
                    case "4" -> {
                        boolean allDone = clientRequestsStatusMap.values().stream()
                                .allMatch(s -> s == Status.DONE);
                        if (! allDone) {
                            System.out.println("There are still requests in progress, are you sure you want to exit? (y/n)");
                            String c = readLine().toLowerCase();
                            if (c.equals("y") || c.equals("yes")) {
                                System.out.println("Exiting");
                                System.exit(0);
                            }
                        }
                    }
                    default -> System.out.println("\nInvalid choice\n");
                }
            }
            catch(Exception e) {
                exceptionHandler.set(e);
                return;
            }
        }
    }

    private static String readLine() {
        StringBuilder input = new StringBuilder();
        try {
            input.append((char) System.in.read());
            while(System.in.available() > 0){
                input.append((char) System.in.read());
            }
            input.deleteCharAt(input.length()-1);
        } catch (IOException ignored) {}
        return input.toString();
    }

    private static void secondaryLoop(Box<Exception> exceptionHandler) {
        while(exceptionHandler.get() == null){
            try{
                checkForFinishedRequests();
            } catch (Exception e){
                exceptionHandler.set(e);
                return;
            }
        }
    }



    private static void checkForFinishedRequests(){
        ReceiveMessageRequest messageRequest = ReceiveMessageRequest.builder()
                .queueUrl(getQueueURL(USER_OUTPUT_QUEUE_NAME))
                .waitTimeSeconds(1)
                .build();

        ReceiveMessageResponse r;
        do{
            r = sqs.receiveMessage(messageRequest);
            if(r.hasMessages()){
                handleFinishedRequests(r.messages());
            }
        } while(r.hasMessages());
    }

    private static void handleFinishedRequests(List<Message> messages) {
        for(Message m: messages){
            CompletedClientRequest completedRequest = JsonUtils.deserialize(m.body(),CompletedClientRequest.class);
            if(completedRequest.clientId().equals(clientId)){
                clientRequestsStatusMap.put(completedRequest.requestId(),Status.DONE);
                String output = downloadFromS3(completedRequest.output());
                createHtmlFile(output,clientRequestMap.get(completedRequest.requestId()).fileName());
                deleteFromQueue(m,USER_OUTPUT_QUEUE_NAME);
                newFinishedRequest.set(true);
            }
        }
    }

    private static void deleteFromQueue(Message message, String queueName) {
        sqs.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(getQueueURL(queueName))
                .receiptHandle(message.receiptHandle())
                .build());
    }



    private static void createHtmlFile(String output, String fileName) {

        String[] jsons = output.split("\n");

        List<TitleReviews> titleReviews = Arrays.stream(jsons)
                .map(tr -> JsonUtils.<TitleReviews>deserialize(tr, TitleReviews.class))
                .toList();

        String[] baseRowParts = BASE_HTML_ROW.split("%s");

        List<String> rows = new LinkedList<>();
        for(TitleReviews tr: titleReviews){
            for(Review r: tr.reviews()){
                rows.add(baseRowParts[0] + getBackgroundColor(r.sentiment()) + baseRowParts[1] +
                        tr.title() + baseRowParts[2] +
                        r.link() + baseRowParts[3] +
                        r.link() + baseRowParts[4] +
                        r.entitiesToString() + baseRowParts[5] +
                        isSarcasm(r.sentiment(),r.rating()) + baseRowParts[6]);
            }
        }

        String[] baseHtmlDocParts = BASE_HTML_DOC.split("%s");
        StringBuilder docBuilder = new StringBuilder();
        docBuilder.append(baseHtmlDocParts[0]).append(fileName).append(baseHtmlDocParts[1]).append(fileName).append(baseHtmlDocParts[2]);
        for(String row: rows){
            docBuilder.append(row).append("\n");
        }
        docBuilder.append(baseHtmlDocParts[3]);

        String pathToWrite = getFolderPath() + "/output files/" + fileName.substring(0, fileName.lastIndexOf(".")) + ".html";
        File file = new File(pathToWrite);
        file.getParentFile().mkdirs();
        try(BufferedWriter writer = new BufferedWriter(new FileWriter(pathToWrite))){
                writer.write(docBuilder.toString());
            } catch (IOException e) {
                log("Failed to write html file, "+ e);
            }
    }

    private static String getBackgroundColor(Review.Sentiment sentiment) {

        Color veryNegative = new Color(110, 1, 1);
        Color negative = new Color(255, 51, 51, 255);
        Color neutral = new Color(0, 0, 0);
        Color positive = new Color(68, 232, 66);
        Color veryPositive = new Color(2, 77, 0);

        return switch(sentiment){
            case VeryNegative -> colorToHex(veryNegative);
            case Negative -> colorToHex(negative);
            case Neutral -> colorToHex(neutral);
            case Positive -> colorToHex(positive);
            case VeryPositive -> colorToHex(veryPositive);
        };
    }

    public static String colorToHex(Color color) {
        int red = color.getRed();
        int green = color.getGreen();
        int blue = color.getBlue();

        return String.format("#%02X%02X%02X", red, green, blue);
    }


    private static String isSarcasm(Review.Sentiment sentiment, int rating) {
        if(sentiment.ordinal() >= 2 && rating < 3) return "Yes";
        if(sentiment.ordinal() < 2 && rating >= 3) return "Yes";
        return "No";
    }

    private static void log(String message){
        if(debugMode){
            String timeStamp = getTimeStamp(LocalDateTime.now());
            // TODO: add log to file
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

    private static void openFinishedRequest() {
        System.out.println("Enter request id:");
        System.out.print(">> ");
        String requestIdStr = readLine();
        int requestId;

        // get a valid request id
        try{
            requestId = Integer.parseInt(requestIdStr);
        } catch (NumberFormatException e){
            System.out.println("\nInvalid request id\n");
            return;
        }

        if(clientRequestsStatusMap.get(requestId) == Status.DONE){
            String path = getFolderPath() + "/output files/" + clientRequestMap.get(requestId).fileName().substring(0, clientRequestMap.get(requestId).fileName().lastIndexOf(".")) + ".html";
            try {
                Desktop.getDesktop().open(new File(path));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void showRequests() {
        TablePrinter table = new TablePrinter("Request id","File name","Status");
        for (Map.Entry<Integer, ClientRequest> entry : clientRequestMap.entrySet()) {
            table.addEntry(entry.getKey().toString(),
                    entry.getValue().fileName(),
                    clientRequestsStatusMap.get(entry.getKey()).toString());
        }
        System.out.println(table);
        waitForEnter();
    }

    private static void waitForEnter() {
        System.out.println("Press enter to continue");
        try {
            System.in.read();
            while(System.in.available() > 0){
                System.in.read();
            }
        } catch (IOException ignored) {}
    }

    private static void sendNewRequest() {
        System.out.print("File name: ");
        String fileName = readLine();
        System.out.print("Reviews per worker: ");
        String reviewsPerWorkerStr = readLine();
        int reviewsPerWorker;
        try{
            reviewsPerWorker = Integer.parseInt(reviewsPerWorkerStr);
        } catch (NumberFormatException e){
            System.out.println("\nInvalid number of reviews\n");
            return;
        }
        System.out.print("Terminate(t/f): ");
        String terminateStr = readLine();
        Boolean terminate = terminateStr.equals("t") ? Boolean.TRUE : terminateStr.equals("f") ? Boolean.FALSE : null;
        if(terminate == null){
            System.out.println("\nInvalid terminate value\n");
            return;
        }
        try {
            sendClientRequest(fileName,reviewsPerWorker,terminate);
        } catch (IOException e) {
            if(e instanceof FileNotFoundException) {
                System.out.println("\nFile not found\n");
            } else {
                log("Failed to send request, "+ e);
            }
            return;
        }
        startManagerIfNotExists();
        System.out.println("\nRequest sent successfully.");
        waitForEnter();
    }

    private static void sendClientRequest(String fileName, int reviewsPerWorker, boolean terminate) throws IOException {
        String input = readInputFile(fileName);
        String pathInS3 = "temp/%s/%s___%s".formatted(clientId, UUID.randomUUID(), fileName);
        uploadToS3(pathInS3, input);
        ClientRequest toSend = new ClientRequest(clientId, requestId, pathInS3, reviewsPerWorker, terminate);
        ClientRequest toSave = new ClientRequest(clientId, requestId, fileName, reviewsPerWorker, terminate);
        clientRequestMap.put(requestId, toSave);
        clientRequestsStatusMap.put(requestId, Status.IN_PROGRESS);
        requestId++;
        sqs.sendMessage(SendMessageRequest.builder()
                      .queueUrl(getQueueURL(USER_INPUT_QUEUE_NAME))
                      .messageBody(JsonUtils.serialize(toSend))
            .build());
    }

    private static void startManagerIfNotExists() {

        if(noManager) return;

        var r = ec2.describeInstances(DescribeInstancesRequest.builder()
                .filters(Filter.builder()
                        .name("tag:Name")
                        .values("ManagerInstance")
                        .build(),
                        Filter.builder()
                                .name("instance-state-name")
                                .values("running")
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
                java -jar managerProgram.jar %s > output.log 2>&1
                sudo shutdown -h now""".formatted(debugFlags);
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

    public static String readInputFile(String fileName) throws IOException {
        String path = getFolderPath()+"/input files/"+fileName;
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        try(BufferedReader buffReader =  new BufferedReader(new FileReader(path))){
            while((line = buffReader.readLine())!=null) {
                stringBuilder.append(line).append("\n");
            }
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

    private static void printUsageAndExit(String errorMessage) {
        if(! errorMessage.equals("")) {
            System.out.println(errorMessage);
        }
        System.out.println(USAGE);
        System.exit(1);
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
        argsList.add("-nomanager");

        for (int i = 0; i < args.length; i++) {
            String arg = args[i].toLowerCase();
            String errorMessage;

            if (debugModeOptions.contains(arg)) {
                debugMode = true;
                continue;
            }
            if (uploadLogOptions.contains(arg)) {
                uploadLogs = true;
                continue;
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
            if(arg.equals("-nomanager")){
                noManager = true;
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
