package kvStore;

import kvStore.fileStore.SSTableManager;
import kvStore.memStore.MemTable;

public class StorageEngine {
    private final MemTable memTable;
    private final SSTableManager ssTableManager;

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
}
