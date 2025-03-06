package kvStore.fileStore;

import java.io.*;
import java.util.*;

public class SSTableManager {
    private final String directory;
    private final List<File> sstables = new ArrayList<>();

    // Constructor now accepts a directory path
    public SSTableManager(String directory) {
        this.directory = directory;
        loadExistingSSTables();
    }

    // Loads existing SSTable files from the specified directory
    private void loadExistingSSTables() {
        File dir = new File(directory);
        if (!dir.exists()) {
            //TODO - handle output
            dir.mkdirs();
        }
        File[] files = dir.listFiles((d, name) -> name.startsWith("sstable_") && name.endsWith(".txt"));
        if (files != null) {
            sstables.addAll(Arrays.asList(files));
        }
    }

    // Writes the given key-value map to a new SSTable file
    public synchronized void writeToSSTable(Map<String, String> data) {
        try {
            String filename = "sstable_" + System.currentTimeMillis() + ".txt";
            File file = new File(directory, filename);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                for (Map.Entry<String, String> entry : data.entrySet()) {
                    writer.write(entry.getKey() + "," + entry.getValue());
                    writer.newLine();
                }
            }
            sstables.add(file);
        } catch (IOException e) {
            throw new RuntimeException("Error writing SSTable", e);
        }
    }

    // Reads the value for a key from SSTables by scanning from newest to oldest
    public synchronized String readFromSSTables(String key) {
        for (int i = sstables.size() - 1; i >= 0; i--) {
            File file = sstables.get(i);
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",", 2);
                    if (parts[0].equals(key)) {
                        return parts[1];
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Error reading SSTable", e);
            }
        }
        return null;
    }

    // Reads the range of values for provided keys from SSTables by scanning from newest to oldest
    public synchronized Map<String, String> readKeyRange(String startKey, String endKey) {
        Map<String, String> result = new TreeMap<>();
        // Iterate from newest to oldest: keys found earlier override older values.
        for (int i = sstables.size() - 1; i >= 0; i--) {
            File file = sstables.get(i);
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",", 2);
                    if (parts.length == 2) {
                        String key = parts[0];
                        String value = parts[1];
                        if (key.compareTo(startKey) >= 0 && key.compareTo(endKey) <= 0) {
                            // Only add if not already present (newer values override older ones).
                            result.putIfAbsent(key, value);
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Error reading SSTable for range", e);
            }
        }
        return result;
    }

    /**
     * Compacts all existing SSTables into a single SSTable.
     * @param tombstones A set of keys that are marked as deleted.
     * @return count of SsTables after compaction
     */
    public synchronized int compact(Set<String> tombstones) {
        // 1. Merge all key-value pairs from every SSTable into one map.
        Map<String, String> mergedData = new TreeMap<>();
        for (File file : sstables) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",", 2);
                    if (parts.length == 2) {
                        String key = parts[0];
                        String value = parts[1];
                        // Skip keys that are marked as deleted
                        if (tombstones.contains(key)) continue;
                        mergedData.put(key, value);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Error during compaction", e);
            }
        }

        // 2. Delete all old SSTable files.
        for (File file : sstables) {
            //TODO - handle output
            file.delete();
        }
        sstables.clear();

        // 3. Write the merged data into a new SSTable.
        writeToSSTable(mergedData);

        // 4. Reload the SSTables (should now contain only the new compacted file).
        loadExistingSSTables();

        return sstables.size();
    }
}
