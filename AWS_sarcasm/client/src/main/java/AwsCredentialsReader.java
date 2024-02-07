import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class AwsCredentialsReader {

    private static final String fileName = "credentials.txt";

    private final Map<String,String> credentialsMap;

    public AwsCredentialsReader() throws FileNotFoundException {
        try {
            credentialsMap = new HashMap<>();

            String pathToFile = getFolderPath() + fileName;
            try (BufferedReader varsFile = new BufferedReader(new FileReader(pathToFile))) {
                String line;
                while ((line = varsFile.readLine()) != null){
                    if(line.contains("=")){
                        //break the lines into key & value
                        String key = line.substring(0,line.indexOf("=")).strip();
                        String value = line.substring((line.indexOf("=")+1)).strip();
                        credentialsMap.put(key,value);
                    }
                }
            }
        } catch (IOException e) {
            if(e instanceof FileNotFoundException fnf){
                throw fnf;
            }
            throw new RuntimeException(e);
        }
    }

    private String getFolderPath() {
        String folderPath = AwsCredentialsReader.class.getResource("AwsCredentialsReader.class").getPath();
        folderPath = folderPath.replace("%20"," "); //fix space character
        folderPath = folderPath.substring(folderPath.indexOf("/")+1); // remove initial '/'
        folderPath = folderPath.substring(0,folderPath.lastIndexOf("/")); // remove .class file from path
        folderPath = folderPath.substring(0,folderPath.lastIndexOf("/")+1); // exit jar
        folderPath = folderPath.replace("/","\\");
        return folderPath;
    }

    public AwsSessionCredentials getCredentials(){
        return AwsSessionCredentials.create(getAccessKeyId(),getSecretAccessKey(),getSessionToken());
    }

    private String getAccessKeyId(){
        return credentialsMap.get("aws_access_key_id");
    }

    private String getSecretAccessKey(){
        return credentialsMap.get("aws_secret_access_key");
    }

    private String getSessionToken(){
        return credentialsMap.get("aws_session_token");
    }

}

