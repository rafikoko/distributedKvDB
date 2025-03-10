package kvStore.memStore;

import kvStore.fileStore.SSTableManager;
import kvStore.log.WriteAheadLog;
import org.junit.jupiter.api.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

public class MemTableWALIntegrationTest {

    private Path tempDir;
    private SSTableManager ssTableManager;
    private WriteAheadLog wal;
    private MemTable memTable;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("memtable_wal_test");
        ssTableManager = new SSTableManager(tempDir.toString());
        wal = new WriteAheadLog(tempDir.toString());
        memTable = new MemTable(ssTableManager, wal);
    }

    @AfterEach
    void tearDown() throws IOException {
        wal.close();
        Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @Test
    void testPutAndDeleteWithWAL_ContentValidation() throws InterruptedException {
        // Insert some operations.
        memTable.put("alpha", "A");
        memTable.put("beta", "B");
        memTable.delete("alpha");

        // Validate WAL content before flush.
        List<WriteAheadLog.LogEntry> preFlushEntries = wal.recover();
        // Expecting three operations: PUT alpha, PUT beta, DELETE alpha.
        assertEquals(3, preFlushEntries.size(), "WAL should contain 3 entries before flush");

        memTable.flush();

        // After flush, the in-memory store is cleared and WAL is rotated.
        assertNull(memTable.get("beta"), "Store should be empty after flush");

        // Validate WAL content after flush.
        List<WriteAheadLog.LogEntry> postFlushEntries = wal.recover();
        postFlushEntries.forEach(System.out::println);
        // The new WAL should be empty (or contain only operations after the flush if any were added).
        assertTrue(postFlushEntries.isEmpty(), "WAL should be empty after flush rotation");
    }

    @Test
    void testRecoveryFromWAL() throws IOException {
        // Simulate operations without flushing.
        memTable.put("one", "1");
        memTable.put("two", "2");
        memTable.delete("one");

        // Simulate a crash by creating a new WAL instance that points to the same file.
        WriteAheadLog walForRecovery = new WriteAheadLog(tempDir.toString());
        // Create a new MemTable instance using the recovered WAL.
        MemTable recoveredMemTable = new MemTable(ssTableManager, walForRecovery);
        recoveredMemTable.recoverFromWAL();

        // After recovery, "one" should be deleted and "two" should be present.
        assertNull(recoveredMemTable.get("one"), "Key 'one' should be deleted after recovery");
        assertEquals("2", recoveredMemTable.get("two"), "Key 'two' should be recovered with value '2'");
    }
}