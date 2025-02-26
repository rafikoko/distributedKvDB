package kvStore.memStore;

public interface KeyValueStore {
    void put(String key, String value);
    String get(String key);
    void delete(String key);
}