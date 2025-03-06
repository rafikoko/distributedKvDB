package kvStore;

import kvStore.fileStore.SSTableManager;
import kvStore.memStore.MemTable;

import java.io.File;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class StorageEnginePerformanceTest {

    public static void main(String[] args) throws Exception {
        // Create a temporary directory for SSTable files.
        Path tempDir = Files.createTempDirectory("storage_engine_perf_test");
        System.out.println("Using temporary directory: " + tempDir.toString());

        // Initialize components.
        SSTableManager ssTableManager = new SSTableManager(tempDir.toString());
        MemTable memTable = new MemTable(ssTableManager);
        StorageEngine storageEngine = new StorageEngine(memTable, ssTableManager);

        int numOperations = 100_000; // Number of operations to perform.
        System.out.println("Performing " + numOperations + " put operations...");

        // Measure put throughput using batchPut.
        long putStartTime = System.nanoTime();
        for (int i = 0; i < numOperations; i++) {
            // Insert one key at a time for simplicity.
            storageEngine.put("key" + i, "value" + i);
        }
        long putEndTime = System.nanoTime();
        double putDurationMs = (putEndTime - putStartTime) / 1_000_000.0;
        double putsPerSecond = numOperations / (putDurationMs / 1000.0);
        System.out.printf("Put throughput: %.2f ops/sec (%.2f ms total)%n", putsPerSecond, putDurationMs);

        // Measure get throughput.
        System.out.println("Performing " + numOperations + " get operations...");
        long getStartTime = System.nanoTime();
        AtomicInteger foundCount = new AtomicInteger();
        for (int i = 0; i < numOperations; i++) {
            String value = storageEngine.get("key" + i);
            foundCount.incrementAndGet();
            if (foundCount.get() % 10000 == 0) {
                System.out.println("Performed already "  + foundCount + "/" + numOperations + " get operations...");
            }
        }
        long getEndTime = System.nanoTime();
        double getDurationMs = (getEndTime - getStartTime) / 1_000_000.0;
        double getsPerSecond = numOperations / (getDurationMs / 1000.0);
        System.out.printf("Get throughput: %.2f ops/sec (%.2f ms total), found %d keys%n",
                getsPerSecond, getDurationMs, foundCount.get());

        // Trigger manual compaction and measure its duration.
        System.out.println("Triggering manual compaction...");
        long compactStartTime = System.nanoTime();
        storageEngine.compactSSTables();
        long compactEndTime = System.nanoTime();
        double compactDurationMs = (compactEndTime - compactStartTime) / 1_000_000.0;
        System.out.printf("Compaction completed in: %.2f ms%n", compactDurationMs);

        // Cleanup: delete the temporary directory and its contents.
        Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        System.out.println("Temporary directory cleaned up.");
    }
}
