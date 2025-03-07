package kvStore.fileStore;

import kvStore.bloomFilter.BloomFilter;

import java.io.*;
import java.util.*;

public class SSTableManager {
    private final String directory;
    private final List<File> sstables = new ArrayList<>();
    private final Map<String, BloomFilter<String>> bloomFilterForFile = new HashMap<>();
    private static final String TOMBSTONE_MARKER = "__TOMBSTONE__";

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
            // Sort files by timestamp (extracted from filename) in ascending order.
            Arrays.sort(files, Comparator.comparingLong(this::extractTimestamp));
            // Now add them so that the list order is from oldest to newest.
            sstables.addAll(Arrays.asList(files));
            // For each SSTable file, attempt to load the corresponding Bloom filter metadata.
            for (File file : sstables) {
                BloomFilter<String> bloom = loadBloomFilterForFile(file);
                if (bloom != null) {
                    bloomFilterForFile.put(file.getName(), bloom);
                }
            }
        }
    }

    // Helper method to load a Bloom filter from a metadata file.
    private BloomFilter<String> loadBloomFilterForFile(File sstableFile) {
        String metadataFilename = sstableFile.getName().replace(".txt", ".bf");
        File metadataFile = new File(directory, metadataFilename);
        if (!metadataFile.exists()) {
            return null;
        }
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(metadataFile))) {
            return (BloomFilter<String>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Failed to load Bloom filter for " + sstableFile.getName() + ": " + e.getMessage());
            return null;
        }
    }

    // Helper method to extract timestamp from the filename
    private long extractTimestamp(File file) {
        // Assuming filename format: "sstable_<timestamp>.txt"
        String name = file.getName();
        try {
            int start = name.indexOf('_') + 1;
            int end = name.lastIndexOf('.');
            return Long.parseLong(name.substring(start, end));
        } catch (Exception e) {
            return 0L; // fallback, though ideally this should not happen
        }
    }

    public synchronized void writeToSSTable(Map<String, String> data) {
        writeToSSTable(data, Collections.emptyMap());
    }

    // New method that accepts tombstones as well.
    public synchronized void writeToSSTable(Map<String, String> data, Map<String, Boolean> tombstones) {
        try {
            String filename = "sstable_" + System.currentTimeMillis() + ".txt";
            File file = new File(directory, filename);

            // Create a Bloom filter sized for the number of keys in data.
            BloomFilter<String> bloomFilter = new BloomFilter<>(data.size(), 0.01); // e.g., 1% false positive rate
            String metadataFilename = filename.replace(".txt", ".bf");
            File metadataFile = new File(directory, metadataFilename);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                // Write live data
                for (Map.Entry<String, String> entry : data.entrySet()) {
                    writer.write(entry.getKey() + "," + entry.getValue());
                    writer.newLine();
                    bloomFilter.add(entry.getKey());
                }
                // Write tombstone entries
                for (String key : tombstones.keySet()) {
                    writer.write(key + "," + TOMBSTONE_MARKER);
                    writer.newLine();
                    bloomFilter.add(key);
                }
            }

            // Serialize and save the Bloom filter to the metadata file.
            try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(metadataFile))) {
                oos.writeObject(bloomFilter);
            }

            sstables.add(file);
            bloomFilterForFile.put(file.getName(), bloomFilter);
        } catch (IOException e) {
            throw new RuntimeException("Error writing SSTable", e);
        }
    }

    // Reads the value for a key from SSTables by scanning from newest to oldest
    public synchronized String readFromSSTables(String key) {
        for (int i = sstables.size() - 1; i >= 0; i--) {
            File file = sstables.get(i);
            BloomFilter<String> bloom = bloomFilterForFile.get(file.getName());  // Method to retrieve the Bloom filter for this file.
            if (bloom != null && !bloom.contains(key)) {
                // Key is definitely not in this file, skip it.
                continue;
            }
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",", 2);
                    if (parts.length == 2 && parts[0].equals(key)) {
                        if (parts[1].equals(TOMBSTONE_MARKER)) {
                            return null; // Key was deleted.
                        }
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
                            // If a tombstone is encountered, skip this key.
                            if (value.equals(TOMBSTONE_MARKER)) continue;
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
        // Iterate from newest to oldest:
        for (int i = sstables.size() - 1; i >= 0; i--) {
            File file = sstables.get(i);
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",", 2);
                    if (parts.length == 2) {
                        String key = parts[0];
                        String value = parts[1];
                        // Skip keys that are marked as deleted
                        if (tombstones.contains(key)) continue;
                        // Only add if this key has not been added yet.
                        if (!mergedData.containsKey(key)) {
                            // If the most recent record for the key is a tombstone, we mark delete it.
                            if (value.equals(TOMBSTONE_MARKER)) {
                                mergedData.remove(key);
                            } else {
                                mergedData.put(key, value);
                            }
                        }
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
