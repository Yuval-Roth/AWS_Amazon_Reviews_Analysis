import java.io.*;

public class createHugeFile {
    public static void main(String[] args) {
        try {

            //read some file
            String input = readInputFile("input.txt");
            File outputF = new File(getFolderPath()+ "bigFile.txt");
            try (FileWriter output = new FileWriter(outputF,true)) {
                for (int i = 0; i < 2500; i++) {
                    output.write(input);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static String readInputFile(String fileName) throws IOException {
        String path = getFolderPath() + fileName;
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
        folderPath = folderPath.substring(0,folderPath.lastIndexOf("/")+1); // exit jar
        return folderPath;
    }

}
