package kvStore.fileStore;

import org.junit.jupiter.api.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

public class SSTableManagerTest {
    private Path tempDir;
    private SSTableManager ssTableManager;

    @BeforeEach
    void setup() throws IOException {
        // Create a temporary directory for testing SSTables.
        tempDir = Files.createTempDirectory("sstable_test");
        ssTableManager = new SSTableManager(tempDir.toString());
    }

    @AfterEach
    void tearDown() throws IOException {
        // Clean up temporary files and directory.
        Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete); //TODO - handle output
    }

    @Test
    void testWriteAndRead() {
        // Create a small data map and write it to an SSTable.
        Map<String, String> data = new HashMap<>();
        data.put("key1", "value1");
        data.put("key2", "value2");
        ssTableManager.writeToSSTable(data);

        // Verify that the values can be read back correctly.
        assertEquals("value1", ssTableManager.readFromSSTables("key1"));
        assertEquals("value2", ssTableManager.readFromSSTables("key2"));
    }

    @Test
    void testReadNonExistentKey() {
        // Write some data to an SSTable.
        Map<String, String> data = new HashMap<>();
        data.put("key1", "value1");
        ssTableManager.writeToSSTable(data);

        // Verify that a non-existent key returns null.
        assertNull(ssTableManager.readFromSSTables("nonexistent"));
    }

    @Test
    void testCompaction() {
        // Write data into two separate SSTables.
        Map<String, String> data1 = new HashMap<>();
        data1.put("key1", "value1");
        data1.put("key2", "value2");
        ssTableManager.writeToSSTable(data1);

        Map<String, String> data2 = new HashMap<>();
        // Simulate an update to key2 and a new key3.
        data2.put("key2", "value2_new");
        data2.put("key3", "value3");
        ssTableManager.writeToSSTable(data2);

        // Verify initial reads before compaction.
        assertEquals("value1", ssTableManager.readFromSSTables("key1"));
        // Newer SSTable should override older values.
        assertEquals("value2_new", ssTableManager.readFromSSTables("key2"));
        assertEquals("value3", ssTableManager.readFromSSTables("key3"));

        // Create a tombstones set to simulate deletion of key2.
        Set<String> tombstones = new HashSet<>();
        tombstones.add("key2");

        // Trigger compaction; this should merge SSTables and remove key2.
        ssTableManager.compact(tombstones);

        // After compaction, key2 should no longer be available.
        assertNull(ssTableManager.readFromSSTables("key2"));
        // Other keys should be preserved.
        assertEquals("value1", ssTableManager.readFromSSTables("key1"));
        assertEquals("value3", ssTableManager.readFromSSTables("key3"));
    }

    @Test
    void testBloomFilterPersistence() {
        // Write data to an SSTable.
        Map<String, String> data = new HashMap<>();
        data.put("key1", "value1");
        data.put("key2", "value2");
        ssTableManager.writeToSSTable(data);

        // Simulate a system restart by creating a new SSTableManager using the same directory.
        SSTableManager reloadedManager = new SSTableManager(tempDir.toString());

        // Verify that the Bloom filter is loaded and used correctly.
        assertEquals("value1", reloadedManager.readFromSSTables("key1"));
        assertEquals("value2", reloadedManager.readFromSSTables("key2"));
        // Check that a non-existent key returns null.
        assertNull(reloadedManager.readFromSSTables("key3"));
    }

    @Test
    void testReadKeyRange() {
        // Prepare a sorted data set.
        Map<String, String> data = new TreeMap<>();
        data.put("a", "alpha");
        data.put("b", "bravo");
        data.put("c", "charlie");
        data.put("d", "delta");
        data.put("e", "echo");

        // Write the data to an SSTable.
        ssTableManager.writeToSSTable(data);

        // Read a range from "b" to "d" (inclusive).
        Map<String, String> rangeResult = ssTableManager.readKeyRange("b", "d");

        // Verify that the range contains the expected keys and values.
        assertEquals(3, rangeResult.size());
        assertEquals("bravo", rangeResult.get("b"));
        assertEquals("charlie", rangeResult.get("c"));
        assertEquals("delta", rangeResult.get("d"));
    }

}
