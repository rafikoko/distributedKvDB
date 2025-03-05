package kvStore;

import kvStore.fileStore.SSTableManager;
import kvStore.memStore.MemTable;
import org.junit.jupiter.api.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;
import static org.junit.jupiter.api.Assertions.*;

public class StorageEngineIntegrationTest {
    private Path tempDir;
    private MemTable memTable;
    private StorageEngine storageEngine;

    @BeforeEach
    void setUp() throws IOException {
        // Create a temporary directory for SSTable files
        tempDir = Files.createTempDirectory("storage_engine_test");
        SSTableManager ssTableManager = new SSTableManager(tempDir.toString());
        memTable = new MemTable(ssTableManager);
        storageEngine = new StorageEngine(memTable, ssTableManager);
    }

    @AfterEach
    void tearDown() throws IOException {
        // Stop background compaction if running
        storageEngine.stopBackgroundCompaction();
        // Recursively delete temporary directory and files
        Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @Test
    void testPutGetDeleteAndManualCompaction() {
        // Insert some keys into the MemTable
        memTable.put("key1", "value1");
        memTable.put("key2", "value2");
        memTable.put("key3", "value3");

        // Verify values are retrievable via the StorageEngine
        assertEquals("value1", storageEngine.get("key1"));
        assertEquals("value2", storageEngine.get("key2"));
        assertEquals("value3", storageEngine.get("key3"));

        // Delete key2; the MemTable should mark it with a tombstone
        memTable.delete("key2");
        assertNull(storageEngine.get("key2"));

        // Trigger manual compaction; tombstones from MemTable should be respected
        storageEngine.compactSSTables();

        // After compaction, key2 remains deleted, while others are preserved
        assertNull(storageEngine.get("key2"));
        assertEquals("value1", storageEngine.get("key1"));
        assertEquals("value3", storageEngine.get("key3"));
    }

    @Test
    void testBackgroundCompaction() throws InterruptedException {
        // Start background compaction to run every 500ms
        storageEngine.startBackgroundCompaction(500);

        // Insert a batch of keys to simulate flush conditions
        for (int i = 0; i < 2500; i++) {
            memTable.put("key" + i, "value" + i);
        }

        // Delete a few specific keys
        memTable.delete("key100");
        memTable.delete("key500");
        memTable.delete("key1500");

        // Allow time for background compaction to trigger
        Thread.sleep(1500);

        // Deleted keys should be absent after compaction
        assertNull(storageEngine.get("key100"));
        assertNull(storageEngine.get("key500"));
        assertNull(storageEngine.get("key1500"));

        // Other keys should remain accessible
        assertEquals("value0", storageEngine.get("key0"));
        assertEquals("value1000", storageEngine.get("key1000"));
        assertEquals("value1499", storageEngine.get("key1499"));
        assertEquals("value2499", storageEngine.get("key2499"));
        assertEquals("value1501", storageEngine.get("key1501"));
    }
}
