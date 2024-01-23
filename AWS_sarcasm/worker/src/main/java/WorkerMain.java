
import java.io.*;
import java.util.Map;
import java.util.stream.Collectors;

public class WorkerMain {

    enum Sentiment{
        VeryNegative,
        Negative,
        Neutral,
        Positive,
        VeryPositive;

        public static Sentiment ofIndex(int index){
            return switch(index){
                case 0 -> Sentiment.VeryNegative;
                case 1 -> Sentiment.Negative;
                case 2 -> Sentiment.Neutral;
                case 3 -> Sentiment.Positive;
                case 4 -> Sentiment.VeryPositive;
                default -> throw new IndexOutOfBoundsException();
            };
        }
    }


    static SentimentAnalysisHandler sentimentAnalysisHandler;
    static NamedEntityRecognitionHandler namedEntityRecognitionHandler;


    public static void main(String[] args){

        File file = new File(args[0]);

        sentimentAnalysisHandler = SentimentAnalysisHandler.getInstance();
        namedEntityRecognitionHandler = NamedEntityRecognitionHandler.getInstance();

        try (BufferedReader reader = new BufferedReader(new FileReader(file))){
            String s = "";
            while((s = reader.readLine()) != null){
                TitleReviews tr = JsonUtils.deserialize(s,TitleReviews.class);
                System.out.println(tr.title());
                for(Review r : tr.reviews()){
                    int sentiment = sentimentAnalysisHandler.findSentiment(r.text());
                    Map<String,String> entities = namedEntityRecognitionHandler.printEntities(r.text())
                            .entrySet().stream()
                            .filter(e -> !e.getValue().equals("O"))
                            .collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));

                    System.out.println(r);
                    System.out.println("Sentiment: "+Sentiment.ofIndex(sentiment));
                    System.out.println("Entities: "+entities);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
