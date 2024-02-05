import java.util.Map;
import java.util.Objects;

public final class Review {
    private final String id;
    private final String link;
    private final String title;
    private final String text;
    private final int rating;
    private final String author;
    private final String date;
    private Sentiment sentiment;
    private Map<String, String> entities;

    public Review(String id, String link, String title, String text, int rating, String author, String date,
                  Sentiment sentiment, Map<String, String> entities) {
        this.id = id;
        this.link = link;
        this.title = title;
        this.text = text;
        this.rating = rating;
        this.author = author;
        this.date = date;
        this.sentiment = sentiment;
        this.entities = entities;
    }

    public Review(String id, String link, String title, String text, int rating, String author, String date) {
        this(id, link, title, text, rating, author, date, null, null);
    }

    public Review(Review r, Sentiment sentiment, Map<String, String> entities) {
        this(r.id(), r.link(), r.title(), r.text(), r.rating(), r.author(), r.date(), sentiment, entities);
    }

    public enum Sentiment {
        VeryNegative,
        Negative,
        Neutral,
        Positive,
        VeryPositive;

        public static Sentiment ofIndex(int index) {
            return switch (index) {
                case 0 -> Sentiment.VeryNegative;
                case 1 -> Sentiment.Negative;
                case 2 -> Sentiment.Neutral;
                case 3 -> Sentiment.Positive;
                case 4 -> Sentiment.VeryPositive;
                default -> throw new IndexOutOfBoundsException();
            };
        }
    }

    @Override
    public String toString() {
        return """
                {
                    id: %s
                    link: %s
                    title: %s
                    text: %s
                    rating: %d
                    author: %s
                    date: %s
                    sentiment: %s
                    entities: %s
                }""".formatted(id, link, title, text, rating, author, date, sentiment, entities);
    }

    public String id() {
        return id;
    }

    public String link() {
        return link;
    }

    public String title() {
        return title;
    }

    public String text() {
        return text;
    }

    public int rating() {
        return rating;
    }

    public String author() {
        return author;
    }

    public String date() {
        return date;
    }

    public Sentiment sentiment() {
        return sentiment;
    }

    public void setSentiment(Sentiment sentiment) {
        this.sentiment = sentiment;
    }

    public Map<String, String> entities() {
        return entities;
    }

    public String entitiesToString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Map.Entry<String, String> entry : entities.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append(", ");
        }
        if(sb.length() != 1) sb.delete(sb.length() - 2, sb.length());
        sb.append("]");
        return sb.toString();
    }

    public void setEntities(Map<String, String> entities) {
        this.entities = entities;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (Review) obj;
        return Objects.equals(this.id, that.id) &&
                Objects.equals(this.link, that.link) &&
                Objects.equals(this.title, that.title) &&
                Objects.equals(this.text, that.text) &&
                this.rating == that.rating &&
                Objects.equals(this.author, that.author) &&
                Objects.equals(this.date, that.date) &&
                Objects.equals(this.sentiment, that.sentiment) &&
                Objects.equals(this.entities, that.entities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, link, title, text, rating, author, date, sentiment, entities);
    }

}
