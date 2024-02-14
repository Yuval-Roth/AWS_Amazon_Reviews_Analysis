import java.io.*;
import java.util.HashMap;
import java.util.Scanner;

public class Main {

    private static HashMap<String,String> variables;
    private static String fileName;

    public static void main(String[] args) {
        fileName = "credentials.txt";
        try {
            readVariables();

            for(String key : variables.keySet()){

                String[] commands = "setx %s %s /m".formatted(key,variables.get(key)).split(" ");

                System.out.printf("Setting key %s: ", key);
                ProcessBuilder pb = new ProcessBuilder(commands);
                Process process = pb.start();

                // get output
                BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
                BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                String s;
                StringBuilder output = new StringBuilder();

                while ((s = stdError.readLine()) != null){
                    output.append(s).append(" ----> did you run as administrator?");
                }
                while ((s = stdInput.readLine()) != null) {
                    output.append(s);
                }
                System.out.println(output);
            }
            System.out.println("Press enter to exit");
            new Scanner(System.in).nextLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void readVariables() throws IOException {

        variables = new HashMap<>();

        try (BufferedReader varsFile = new BufferedReader(new FileReader(getFolderPath()+fileName))) {
            String line;
            while ((line = varsFile.readLine()) != null){
                if(line.contains("=") && line.charAt(0) != '#'){
                    //break the lines into key & value
                    String key = line.substring(0,line.indexOf("=")).strip().toUpperCase();
                    String value = line.substring((line.indexOf("=")+1)).strip();
                    variables.put(key,value);
                }
            }
        } catch (FileNotFoundException e){
            System.out.println("input file not found");
        }
    }


    private static String getFolderPath() {
        String folderPath = Main.class.getResource("Main.class").getPath();
        folderPath = folderPath.replace("%20"," "); //fix space character
        folderPath = folderPath.substring(folderPath.indexOf("/")+1); // remove initial '/'
        folderPath = folderPath.substring(0,folderPath.lastIndexOf("/")); // remove .class file from path
        folderPath = folderPath.substring(0,folderPath.lastIndexOf("/")); // exit jar
        folderPath = folderPath.substring(0,folderPath.lastIndexOf("/")+1); // one more folder up
        folderPath = folderPath.replace("/","\\");
        System.out.println(folderPath);
        return folderPath;
    }


}

