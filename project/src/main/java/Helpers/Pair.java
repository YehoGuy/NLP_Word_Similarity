package Helpers;

public class Pair <K,V> {
    private final K key;
    private final V value;

    public Pair(K key, V value){
        this.key=key;
        this.value=value;
    }

    public Pair(){
        this.key=null;
        this.value=null;
    }

    public K getKey(){
        return key;
    }

    public V getVal(){
        return value;
    }

    public K first(){
        return key;
    }

    public V second(){
        return value;
    }

}
