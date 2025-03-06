package kvStore.memStore;

import kvStore.fileStore.SSTableManager;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class MemTable implements KeyValueStore {
    private static final int FLUSH_THRESHOLD = 1000; // Arbitrary threshold before flushing
    private final TreeMap<String, String> store = new TreeMap<>(); //Sorted order enables efficient range queries.
    private final Map<String, Boolean> tombstones = new HashMap<>();
    private final SSTableManager ssTableManager;

    public MemTable(SSTableManager ssTableManager) {
        this.ssTableManager = ssTableManager;
    }

    public synchronized void put(String key, String value) {
        store.put(key, value);
        tombstones.remove(key); // If previously marked as deleted, remove the tombstone

        if (store.size() >= FLUSH_THRESHOLD) {
            flush();
        }
    }

    public synchronized void batchPut(Map<String, String> entries) {
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    public synchronized String get(String key) {
        if (tombstones.containsKey(key)) {
            return null; // Deleted key
        }
        return store.get(key); // Only return from in-memory storage
    }

    // Returns a subMap for the given key range, filtering out tombstoned keys.
    public synchronized NavigableMap<String, String> readRange(String startKey, String endKey) {
        NavigableMap<String, String> subMap = store.subMap(startKey, true, endKey, true);
        NavigableMap<String, String> result = new TreeMap<>();
        for (Map.Entry<String, String> entry : subMap.entrySet()) {
            if (!tombstones.containsKey(entry.getKey())) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    public synchronized void delete(String key) {
        store.remove(key);
        tombstones.put(key, true); // Mark as deleted, but do NOT persist this
    }

    private void flush() {

            ssTableManager.writeToSSTable(store);
            store.clear();
            // Do NOT clear tombstones—StorageEngine needs them

    }

    public boolean hasTombstone(String key) {
        return tombstones.containsKey(key);
    }

    public Map<String, Boolean> getTombstones() {
        return tombstones;
    }
}
