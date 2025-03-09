package kvStore.log;

import org.junit.jupiter.api.*;
import java.io.*;
import java.nio.file.*;
import java.util.Comparator;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class WriteAheadLogTest {

    // Use a temporary directory for WAL files during tests.
    private Path tempDir;
    private WriteAheadLog wal;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("wal_test");
        wal = new WriteAheadLog(tempDir.toString());
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
    void testAppendAndRecover() {
        // Append some operations to the WAL.
        wal.appendPut("key1", "value1");
        wal.appendDelete("key2");
        wal.appendPut("key3", "value3");

        // Recover log entries.
        List<WriteAheadLog.LogEntry> entries = wal.recover();
        assertEquals(3, entries.size(), "Expected 3 log entries");

        WriteAheadLog.LogEntry e1 = entries.get(0);
        WriteAheadLog.LogEntry e2 = entries.get(1);
        WriteAheadLog.LogEntry e3 = entries.get(2);

        assertEquals(WriteAheadLog.LogEntry.Operation.PUT, e1.op);
        assertEquals("key1", e1.key);
        assertEquals("value1", e1.value);

        assertEquals(WriteAheadLog.LogEntry.Operation.DELETE, e2.op);
        assertEquals("key2", e2.key);

        assertEquals(WriteAheadLog.LogEntry.Operation.PUT, e3.op);
        assertEquals("key3", e3.key);
        assertEquals("value3", e3.value);
    }

    @Test
    void testRotate() throws InterruptedException {
        // Append an operation.
        wal.appendPut("keyA", "valueA");
        String oldFileName = wal.getLogFile().getName();

        // Rotate the WAL.
        Thread.sleep(5);
        wal.rotate();
        String newFileName = wal.getLogFile().getName();
        assertNotEquals(oldFileName, newFileName, "WAL file name should change after rotation");

        // The new WAL should be empty.
        List<WriteAheadLog.LogEntry> entries = wal.recover();
        assertTrue(entries.isEmpty(), "New WAL should be empty after rotation");

        // Append new operation and verify recovery.
        wal.appendPut("keyB", "valueB");
        entries = wal.recover();
        assertEquals(1, entries.size(), "Expected 1 log entry in new WAL");
        assertEquals("keyB", entries.getFirst().key);
        assertEquals("valueB", entries.getFirst().value);
    }
}