package kvStore.bloomFilter;

public class BloomFilterPerformanceTest {
    public static void main(String[] args) {
        int numElements = 1_000_000;
        double falsePositiveRate = 0.01; // 1% desired false positive probability

        // Create and populate the Bloom filter.
        BloomFilter<String> bloomFilter = new BloomFilter<>(numElements, falsePositiveRate);
        for (int i = 0; i < numElements; i++) {
            bloomFilter.add("element_" + i);
        }
        System.out.println("Bloom filter created with bitset size " + bloomFilter.getBitsetSize()
                + " and " + bloomFilter.getNumHashFunctions() + " hash functions.");

        // Perform membership tests on elements not added to the filter.
        int falsePositives = 0;
        int testCount = 100_000;
        long startTime = System.nanoTime();
        for (int i = numElements; i < numElements + testCount; i++) {
            if (bloomFilter.contains("element_" + i)) {
                falsePositives++;
            }
        }
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;
        System.out.println("Performed " + testCount + " membership tests in " + durationMs + " ms.");
        System.out.println("False positives: " + falsePositives);
    }
}

