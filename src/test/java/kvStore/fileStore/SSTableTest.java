package kvStore.fileStore;

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

    @BeforeEach
    void setUp() {
        ssTable = new SSTable(TEST_FILE);
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
