package kvStore.fileStore;

import java.io.*;
import java.nio.file.*;
import java.util.*;

//sorted string table
public class SSTable {
    final Path filePath;

    public SSTable(String fileName) {
        this.filePath = Paths.get(fileName);
    }

    public void write(Map<String, String> data) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
            for (var entry : data.entrySet()) {
                writer.write(entry.getKey() + "," + entry.getValue());
                writer.newLine();
            }
        }
    }

    public String read(String key) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",", 2);
                if (parts[0].equals(key)) return parts[1];
            }
        }
        return null;
    }

    public List<String> readAllKeys() throws IOException {
        List<String> keys = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                keys.add(line.split(",", 2)[0]);
            }
        }
        return keys;
    }
}

