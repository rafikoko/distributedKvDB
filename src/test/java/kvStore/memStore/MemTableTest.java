package kvStore.memStore;

import kvStore.StorageEngine;
import kvStore.fileStore.SSTableManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class MemTableTest {
    private MemTable memTable;
    private SSTableManager ssTableManager;
    private static final String TEST_DIRECTORY = "test_data/";

    @BeforeEach
    void setup() {
        ssTableManager = new SSTableManager(TEST_DIRECTORY);
        memTable = new MemTable(ssTableManager);
    }

    @Test
    void testPutAndGet() {
        memTable.put("key1", "value1");
        assertEquals("value1", memTable.get("key1"));
    }

    @Test
    void testDelete() {
        memTable.put("key1", "value1");
        memTable.delete("key1");

        assertNull(memTable.get("key1"));
        assertTrue(memTable.hasTombstone("key1")); // Ensure the tombstone is recorded
    }

    @Test
    void testMemTableFlush() {
        for (int i = 0; i < 2000; i++) {
            memTable.put("key" + i, "value" + i);
        }

        // MemTable should have flushed, check if data is still accessible
        assertEquals("value500", ssTableManager.readFromSSTables("key500"));
        assertEquals("value999", ssTableManager.readFromSSTables("key999"));
        assertEquals("value1500", ssTableManager.readFromSSTables("key1500"));
        assertEquals("value1999", ssTableManager.readFromSSTables("key1999"));
        assertNull(memTable.get("key500"));
        assertNull(memTable.get("key999"));
        assertNull(memTable.get("key1500"));
        assertNull(memTable.get("key1999"));
    }

    @Test
    void testStorageEngineRespectsDeletes() {
        memTable.put("key1", "value1");
        memTable.delete("key1");

        StorageEngine storageEngine = new StorageEngine(memTable, ssTableManager);
        assertNull(storageEngine.get("key1"));
    }
}
