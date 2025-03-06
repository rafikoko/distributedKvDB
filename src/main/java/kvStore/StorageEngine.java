package kvStore;

import kvStore.fileStore.SSTableManager;
import kvStore.memStore.MemTable;

import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.Map;

public class StorageEngine {
    private final MemTable memTable;
    private final SSTableManager ssTableManager;
    private ScheduledExecutorService compactionExecutor;

    public StorageEngine(MemTable memTable, SSTableManager ssTableManager) {
        this.memTable = memTable;
        this.ssTableManager = ssTableManager;
    }

    public String get(String key) {
        if (memTable.hasTombstone(key)) {
            return null; // Key was deleted
        }
        String value = memTable.get(key);
        if (value != null) {
            return value;
        }
        return ssTableManager.readFromSSTables(key);
    }

    public int compactSSTables() {
        return ssTableManager.compact(memTable.getTombstones().keySet());
    }

    // New method to support reading a key range.
    public Map<String, String> readKeyRange(String startKey, String endKey) {
        // Get results from MemTable.
        Map<String, String> memRange = memTable.readRange(startKey, endKey);
        // Get results from SSTables.
        Map<String, String> sstableRange = ssTableManager.readKeyRange(startKey, endKey);

        // Merge: MemTable data (if present) overrides SSTable values.
        for (Map.Entry<String, String> entry : memRange.entrySet()) {
            sstableRange.put(entry.getKey(), entry.getValue());
        }
        // Filter out any keys that have been deleted (tombstoned).
        for (String key : memTable.getTombstones().keySet()) {
            sstableRange.remove(key);
        }
        return new TreeMap<>(sstableRange);
    }

    // New method to support batch insertion.
    public void batchPut(Map<String, String> entries) {
        memTable.batchPut(entries);
    }

    /**
     * Starts a background thread that triggers compaction at fixed intervals.
     * @param periodMillis The compaction period in milliseconds.
     */
    public void startBackgroundCompaction(long periodMillis) {
        compactionExecutor = Executors.newSingleThreadScheduledExecutor();
        compactionExecutor.scheduleAtFixedRate(() -> {
            try {
                System.out.println("Background compaction triggered.");
                int ssTablesCount = compactSSTables();
                System.out.println("Number of SSTables after compaction: " + ssTablesCount);
            } catch (Exception e) {
                System.err.println("Compaction error: " + e.getMessage());
            }
        }, periodMillis, periodMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Stops the background compaction thread.
     */
    public void stopBackgroundCompaction() {
        if (compactionExecutor != null) {
            compactionExecutor.shutdown();
        }
    }

}
