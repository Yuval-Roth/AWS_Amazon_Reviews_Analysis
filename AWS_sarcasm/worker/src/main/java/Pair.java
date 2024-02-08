public class Pair<K,V> {

    public K getFirst() {
        return first;
    }

    public void setFirst(K first) {
        this.first = first;
    }

    public V getSecond() {
        return second;
    }

    public void setSecond(V second) {
        this.second = second;
    }

    private K first;
    private V second;


    private Pair(K key, V value) {
        this.first = key;
        this.second = value;
    }

    public static <K,V> Pair<K,V> of(K first, V second) {
        return new Pair<>(first, second);
    }
}
