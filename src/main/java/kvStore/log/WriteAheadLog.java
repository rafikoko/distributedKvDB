package kvStore.log;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class WriteAheadLog {
    private File logFile;
    private BufferedWriter writer;
    private final String directory;

    public File getLogFile() {
        return logFile;
    }

    public WriteAheadLog(String directory) {
        this.directory = directory;
        this.logFile = new File(directory, "wal_" + System.currentTimeMillis() + ".log");
        try {
            // Open the log file in append mode.
            writer = new BufferedWriter(new FileWriter(logFile, true));
        } catch (IOException e) {
            throw new RuntimeException("Error initializing WAL", e);
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

    // Simple escaping to handle commas.
    private String escape(String s) {
        return s.replace(",", "\\,");
    }

    private String unescape(String s) {
        return s.replace("\\,", ",");
    }

    public synchronized void close() {
        try {
            writer.close();
        } catch (IOException e) {
            // Handle error if needed.
        }
    }

    /**
     * Replays the WAL by reading all log entries.
     * Returns a list of log entries in the order they were written.
     */
    public List<LogEntry> recover() {
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

    /**
     * Rotates the WAL: closes the current log file and starts a new one.
     */
    public synchronized void rotate() {
        // Close the current writer.
        close();
        // Optionally, archive or remove the old WAL file if no longer needed.
        // For simplicity, we simply create a new WAL.
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
    }
}
