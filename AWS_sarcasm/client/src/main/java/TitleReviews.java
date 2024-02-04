import java.util.LinkedList;
import java.util.List;

public record TitleReviews(String title, List<Review> reviews) {
    public TitleReviews(String title, List<Review> reviews) {
        this.title = title;
        this.reviews = new LinkedList<>(reviews);
    }
}
