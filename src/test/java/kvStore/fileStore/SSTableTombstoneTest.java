package kvStore.fileStore;

import kvStore.StorageEngine;
import kvStore.memStore.MemTable;
import org.junit.jupiter.api.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

public class SSTableTombstoneTest {
    private Path tempDir;
    private MemTable memTable;
    private StorageEngine storageEngine;

    @BeforeEach
    void setUp() throws IOException {
        // Create a temporary directory for SSTable files.
        tempDir = Files.createTempDirectory("tombstone_test");
        SSTableManager ssTableManager = new SSTableManager(tempDir.toString());
        memTable = new MemTable(ssTableManager);
        storageEngine = new StorageEngine(memTable, ssTableManager);
    }

    @AfterEach
    void tearDown() throws IOException {
        // Stop any background threads if running and clean up temp directory.
        storageEngine.stopBackgroundCompaction();
        Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @Test
    void testTombstonePersistenceOnFlush() {
        // Insert keys and then delete some.
        memTable.put("key1", "value1");
        memTable.put("key2", "value2");
        memTable.put("key3", "value3");
        memTable.delete("key2");

        // Force a flush by reaching the threshold

        // Let's insert extra keys to trigger flush.
        for (int i = 4; i < 1010; i++) {
            memTable.put("key" + i, "value" + i);
        }

        // Now, after flush, the tombstone for "key2" should have been persisted.
        // Simulate a restart by creating a new SSTableManager from the same directory.
        SSTableManager reloadedManager = new SSTableManager(tempDir.toString());
        // Check that reading "key2" returns null (due to the tombstone).
        String result = reloadedManager.readFromSSTables("key2");
        assertNull(result, "Deleted key should not be returned after flush/reload.");

        // Live keys should still be available.
        assertEquals("value1", reloadedManager.readFromSSTables("key1"));
        // And key3 should be retrievable.
        assertEquals("value3", reloadedManager.readFromSSTables("key3"));
        assertEquals("value899", reloadedManager.readFromSSTables("key899"));
    }

    @Test
    void testTombstoneHandlingDuringCompaction() {
        // Write data in two phases to simulate multiple SSTables.
        memTable.put("a", "1");
        memTable.put("b", "2");
        memTable.put("c", "3");
        memTable.delete("b");
        // Force flush.
        for (int i = 4; i < 1010; i++) {
            memTable.put("key" + i, "value" + i);
        }
        // At this point, an SSTable exists with "a", "b"(tombstoned), "c", and many other keys.

        // Now, write additional updates in a new MemTable flush.
        memTable.put("b", "new2");  // Even if reinserted, we then delete it again.
        memTable.delete("b");
        for (int i = 1010; i < 2010; i++) {
            memTable.put("key" + i, "value" + i);
        }

        // Manually trigger compaction.
        storageEngine.compactSSTables();

        // After compaction, "b" should not be present.
        assertNull(storageEngine.get("b"), "Key 'b' should be removed after compaction due to tombstone.");
        // Other keys should remain.
        assertEquals("1", storageEngine.get("a"));
        assertEquals("3", storageEngine.get("c"));
        assertEquals("value899", storageEngine.get("key899"));
        assertEquals("value1899", storageEngine.get("key1899"));
    }

    @Test
    void testTombstoneHandlingDuringCompactionWhenKeyReinserted() {
        // Write data in two phases to simulate multiple SSTables.
        memTable.put("a", "1");
        memTable.put("b", "2");
        memTable.put("c", "3");
        memTable.delete("b");
        // Force flush.
        for (int i = 4; i < 1010; i++) {
            memTable.put("key" + i, "value" + i);
        }
        // At this point, an SSTable exists with "a", "b"(tombstoned), "c", and many other keys.

        // Now, write additional updates in a new MemTable flush.
        memTable.put("b", "new2");  // reinserting
        for (int i = 1010; i < 2010; i++) {
            memTable.put("key" + i, "value" + i);
        }

        // Manually trigger compaction.
        storageEngine.compactSSTables();

        // After compaction, "b" should be present.
        assertEquals("new2", storageEngine.get("b"));
        // Other keys should remain.
        assertEquals("1", storageEngine.get("a"));
        assertEquals("3", storageEngine.get("c"));
        assertEquals("value899", storageEngine.get("key899"));
        assertEquals("value1899", storageEngine.get("key1899"));
    }
}

