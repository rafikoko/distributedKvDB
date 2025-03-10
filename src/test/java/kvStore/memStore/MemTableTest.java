package kvStore.memStore;

import kvStore.StorageEngine;
import kvStore.fileStore.SSTableManager;
import kvStore.log.WriteAheadLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class MemTableTest {
    private WriteAheadLog writeAheadLog;
    private MemTable memTable;
    private SSTableManager ssTableManager;
    private static final String TEST_DIRECTORY = "test_data/";

    @BeforeEach
    void setup() {
        ssTableManager = new SSTableManager(TEST_DIRECTORY);
        writeAheadLog = new WriteAheadLog(TEST_DIRECTORY, true);
        memTable = new MemTable(ssTableManager,writeAheadLog);
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

    @Test
    void testWALRotationAfterFlush() {
        // Insert a few operations.
        memTable.put("alpha", "A");
        memTable.put("beta", "B");
        memTable.delete("alpha");

        // Validate WAL content before flush.
        List<WriteAheadLog.LogEntry> preFlushEntries = writeAheadLog.recover();
        assertEquals(3, preFlushEntries.size(), "WAL should contain 3 entries before flush");

        // Explicitly trigger flush.
        memTable.flush();

        // After flush, the in-memory store is cleared and WAL is rotated.
        // Validate that the new WAL (via wal.recover()) is empty.
        List<WriteAheadLog.LogEntry> postFlushEntries = writeAheadLog.recover();
        assertTrue(postFlushEntries.isEmpty(), "WAL should be empty after flush rotation");
    }

    @Test
    void testNewPutsAfterFlushAreLogged() {
        // Trigger a flush to rotate WAL.
        memTable.put("key1", "value1");
        memTable.flush();

        // Now add a new operation.
        memTable.put("key2", "value2");

        // Verify that the new WAL contains only the new operation.
        List<WriteAheadLog.LogEntry> entries = writeAheadLog.recover();
        assertEquals(1, entries.size(), "New WAL should contain 1 entry after flush and new put");
        WriteAheadLog.LogEntry entry = entries.getFirst();
        assertEquals("key2", entry.key);
        assertEquals("value2", entry.value);
    }


}
