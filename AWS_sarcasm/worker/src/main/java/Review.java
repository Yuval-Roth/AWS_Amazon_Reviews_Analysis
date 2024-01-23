
public record Review (String id, String link, String title, String text, int rating, String author, String date,
                      WorkerMain.Sentiment sentiment) {

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
                    
                }""".formatted(id,link,title,text,rating,author,date);
    }
}
