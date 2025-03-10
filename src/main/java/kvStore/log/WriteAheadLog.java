package kvStore.log;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WriteAheadLog implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private File logFile;  // Made package-private for testing
    private BufferedWriter writer;
    private final String directory;

    public File getLogFile() {
        return logFile;
    }

    public WriteAheadLog(String directory) {
        this.directory = directory;
        // Try to find the most recent WAL file.
        File latestWal = getLatestWalFile();
        if (latestWal != null) {
            // Use the existing WAL file for recovery.
            this.logFile = latestWal;
        } else {
            // No existing WAL found; create a new one.
            this.logFile = new File(directory, "wal_" + System.currentTimeMillis() + ".log");
        }
        try {
            // Open the file in append mode.
            writer = new BufferedWriter(new FileWriter(logFile, true));
        } catch (IOException e) {
            throw new RuntimeException("Error initializing WAL", e);
        }
    }

    // Scans the directory for existing WAL files and returns the one with the highest timestamp.
    private File getLatestWalFile() {
        File dir = new File(directory);
        File[] files = dir.listFiles((d, name) -> name.startsWith("wal_") && name.endsWith(".log"));
        if (files != null && files.length > 0) {
            // Sort files by timestamp in descending order.
            Arrays.sort(files, (f1, f2) -> Long.compare(extractTimestamp(f2.getName()), extractTimestamp(f1.getName())));
            return files[0]; // The most recent WAL file.
        }
        return null;
    }

    // Helper method to extract timestamp from a WAL filename.
    private long extractTimestamp(String filename) {
        try {
            int start = filename.indexOf('_') + 1;
            int end = filename.lastIndexOf('.');
            return Long.parseLong(filename.substring(start, end));
        } catch (Exception e) {
            return 0L;
        }
    }

    public synchronized void appendPut(String key, String value) {
        try {
            writer.write("PUT," + escape(key) + "," + escape(value));
            writer.newLine();
            // Optionally, flush periodically or after a batch.
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException("Error writing PUT to WAL", e);
        }
    }

    public synchronized void appendDelete(String key) {
        try {
            writer.write("DELETE," + escape(key));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException("Error writing DELETE to WAL", e);
        }
    }

    private String escape(String s) {
        return s.replace(",", "\\,");
    }

    private String unescape(String s) {
        return s.replace("\\,", ",");
    }

    public synchronized List<LogEntry> recover() {
        List<LogEntry> entries = new ArrayList<>();
        if (!logFile.exists()) return entries;
        try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Split on commas not preceded by a backslash.
                String[] parts = line.split("(?<!\\\\),");
                if (parts.length >= 2) {
                    String op = parts[0];
                    String key = unescape(parts[1]);
                    if ("PUT".equals(op) && parts.length == 3) {
                        String value = unescape(parts[2]);
                        entries.add(new LogEntry(LogEntry.Operation.PUT, key, value));
                    } else if ("DELETE".equals(op)) {
                        entries.add(new LogEntry(LogEntry.Operation.DELETE, key, null));
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error recovering WAL", e);
        }
        return entries;
    }

    public synchronized void close() {
        try {
            writer.close();
        } catch (IOException e) {
            // Optionally log error.
        }
    }

    /**
     * Rotates the WAL: closes the current log file and starts a new one.
     */
    public synchronized void rotate() {
        // Close the current writer.
        close();
        // Optionally archive or delete the old WAL file here.
        // Create a new WAL file.
        this.logFile = new File(directory, "wal_" + System.currentTimeMillis() + ".log");
        try {
            writer = new BufferedWriter(new FileWriter(logFile, true));
        } catch (IOException e) {
            throw new RuntimeException("Error rotating WAL", e);
        }
    }

    public static class LogEntry {
        public enum Operation { PUT, DELETE }
        public Operation op;
        public String key;
        public String value; // Only used for PUT

        public LogEntry(Operation op, String key, String value) {
            this.op = op;
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return "LogEntry{" +
                    "op=" + op +
                    ", key='" + key + '\'' +
                    ", value='" + value + '\'' +
                    '}';
        }
    }
}