package kvStore;

import kvStore.fileStore.SSTableManager;
import kvStore.memStore.MemTable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
