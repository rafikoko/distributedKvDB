package kvStore.fileStore;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public class SSTableManager {
    private static final String STORAGE_DIR = "data/";
    private final NavigableMap<String, String> index = new TreeMap<>();

    public SSTableManager() throws IOException {
        Files.createDirectories(Paths.get(STORAGE_DIR));
        loadExistingSSTables();
    }

    private void loadExistingSSTables() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(STORAGE_DIR), "*.sst")) {
            for (Path path : stream) {
                indexSSTable(path.toString());
            }
        }
    }

    public synchronized void writeToSSTable(Map<String, String> data) throws IOException {
        String filename = STORAGE_DIR + "sstable_" + System.currentTimeMillis() + ".sst";
        SSTable ssTable = new SSTable(filename);
        ssTable.write(data);
        indexSSTable(filename);
    }

    private void indexSSTable(String filename) throws IOException {
        SSTable ssTable = new SSTable(filename);
        for (String key : ssTable.readAllKeys()) {
            index.put(key, filename);
        }
    }

    public synchronized String readFromSSTables(String key) {
        Map.Entry<String, String> entry = index.floorEntry(key);
        if (entry != null) {
            try {
                return new SSTable(entry.getValue()).read(key);
            } catch (IOException e) {
                throw new RuntimeException("Failed to read from SSTable", e);
            }
        }
        return null;
    }
}
