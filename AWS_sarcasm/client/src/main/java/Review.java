import java.util.Map;
import java.util.Objects;

public record Review(String id, String link, String title, String text, int rating, String author, String date,
                     Review.Sentiment sentiment, Map<String, String> entities) {

    public enum Sentiment {
        VeryNegative,
        Negative,
        Neutral,
        Positive,
        VeryPositive;
    }

    public String entitiesToString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Map.Entry<String, String> entry : entities.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append(", ");
        }
        if (sb.length() != 1) sb.delete(sb.length() - 2, sb.length());
        sb.append("]");
        return sb.toString();
    }
}
