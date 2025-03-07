package kvStore.fileStore;

import kvStore.bloomFilter.BloomFilter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

public class SSTableTest {
    private static final String TEST_FILE = "test_sstable.txt";
    private SSTable ssTable;
    int numElements = 3;
    double falsePositiveRate = 0.01; // 1% desired false positive probability
    // Create and populate the Bloom filter.
    BloomFilter<String> bloomFilter = new BloomFilter<>(numElements, falsePositiveRate);

    @BeforeEach
    void setUp() {
        ssTable = new SSTable(TEST_FILE, bloomFilter);
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.deleteIfExists(ssTable.filePath);
    }

    @Test
    void testWriteAndRead() throws IOException {
        ssTable.write(Map.of("key1", "value1", "key2", "value2"));
        assertEquals("value1", ssTable.read("key1"));
        assertEquals("value2", ssTable.read("key2"));
    }

    @Test
    void testReadNonExistentKey() throws IOException {
        ssTable.write(Map.of("key1", "value1"));
        assertNull(ssTable.read("key3"));
    }
}
