package worker;

public record Review (String id, String link, String title, String text, int rating, String author, String date) {

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
                }""".formatted(id,link,title,text,rating,author,date);
    }
}
