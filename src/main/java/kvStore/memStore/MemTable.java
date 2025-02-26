package kvStore.memStore;

import kvStore.fileStore.SSTableManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
        store.put(key, value);
        tombstones.remove(key); // If previously marked as deleted, remove the tombstone

        if (store.size() >= FLUSH_THRESHOLD) {
            flush();
        }
    }

    public synchronized String get(String key) {
        if (tombstones.containsKey(key)) {
            return null; // Deleted key
        }
        return store.get(key); // Only return from in-memory storage
    }

    public synchronized void delete(String key) {
        store.remove(key);
        tombstones.put(key, true); // Mark as deleted, but do NOT persist this
    }

    private void flush() {
        try {
            ssTableManager.writeToSSTable(store);
            store.clear();
            // Do NOT clear tombstones—StorageEngine needs them
        } catch (IOException e) {
            throw new RuntimeException("Error flushing MemTable to SSTable", e);
        }
    }

    public boolean hasTombstone(String key) {
        return tombstones.containsKey(key);
    }
}
