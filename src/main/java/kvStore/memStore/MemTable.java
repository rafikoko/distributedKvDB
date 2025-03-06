package kvStore.memStore;

import kvStore.fileStore.SSTableManager;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemTable implements KeyValueStore {
    private static final int FLUSH_THRESHOLD = 1000;
    // Use ConcurrentSkipListMap for a thread-safe, sorted map.
    private final ConcurrentSkipListMap<String, String> store = new ConcurrentSkipListMap<>();
    // Use ConcurrentHashMap for tombstones.
    private final ConcurrentHashMap<String, Boolean> tombstones = new ConcurrentHashMap<>();
    private final SSTableManager ssTableManager;

    public MemTable(SSTableManager ssTableManager) {
        this.ssTableManager = ssTableManager;
    }

    public void put(String key, String value) {
        store.put(key, value);
        tombstones.remove(key); // Remove any previous deletion marker.
        if (store.size() >= FLUSH_THRESHOLD) {
            flush();
        }
    }

    public void batchPut(Map<String, String> entries) {
        // For each entry, simply call put()—concurrent maps handle concurrent writes.
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    public String get(String key) {
        // First, check tombstones.
        if (tombstones.containsKey(key)) {
            return null;
        }
        return store.get(key);
    }

    public NavigableMap<String, String> readRange(String startKey, String endKey) {
        // Get a view of the range from the concurrent sorted map.
        NavigableMap<String, String> subMap = store.subMap(startKey, true, endKey, true);
        // Remove keys that have been marked as deleted.
        NavigableMap<String, String> result = new ConcurrentSkipListMap<>();
        for (Map.Entry<String, String> entry : subMap.entrySet()) {
            if (!tombstones.containsKey(entry.getKey())) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    public void delete(String key) {
        store.remove(key);
        tombstones.put(key, true); // Mark key as deleted.
    }

    /**
     * Flushes the current snapshot of the store to disk.
     * Note: This snapshot mechanism is not strictly atomic,
     * so further refinement might be necessary in production.
     */
    private void flush() {
        // Create snapshots of live data and tombstones
        NavigableMap<String, String> dataSnapshot = new TreeMap<>(store);
        Map<String, Boolean> tombstoneSnapshot = new HashMap<>(tombstones);

        // Clear the in-memory store (live data only)
        store.clear();

        // Persist both data and tombstones
        ssTableManager.writeToSSTable(dataSnapshot, tombstoneSnapshot);
        // Tombstones are not cleared here so that the StorageEngine can
        // use them to filter out deleted keys.
    }

    public boolean hasTombstone(String key) {
        return tombstones.containsKey(key);
    }

    public Map<String, Boolean> getTombstones() {
        return new ConcurrentHashMap<>(tombstones);
    }
}
