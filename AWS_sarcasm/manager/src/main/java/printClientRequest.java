public class printClientRequest {

    public static void main(String[] args){
        ClientRequest clientRequest = new ClientRequest("client1", 1,"inputFile.txt",5, false);
        System.out.println(JsonUtils.serialize(clientRequest));
    }
}
