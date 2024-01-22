package worker;

import utils.JsonUtils;

import java.io.*;

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


    static sentimentAnalysisHandler sentimentAnalysisHandler = new sentimentAnalysisHandler();
    static namedEntityRecognitionHandler namedEntityRecognitionHandler = new namedEntityRecognitionHandler();


    public static void main(String[] args){

        File file = new File(args[0]);
        try (BufferedReader reader = new BufferedReader(new FileReader(file))){
            String s = "";
            while((s = reader.readLine()) != null){
                TitleReviews tr = JsonUtils.deserialize(s,TitleReviews.class);
                System.out.println(tr.title());
                for(Review r : tr.reviews()){
                    int sentiment = worker.sentimentAnalysisHandler.findSentiment(r.text());
                    System.out.println(r);
                    System.out.println(Sentiment.ofIndex(sentiment));
                    worker.namedEntityRecognitionHandler.printEntities(r.text());
                }
            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
