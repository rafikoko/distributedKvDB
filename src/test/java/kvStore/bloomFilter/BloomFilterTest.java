package kvStore.bloomFilter;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.*;

public class BloomFilterTest {

    @Test
    public void testAddAndContains() {
        BloomFilter<String> bf = new BloomFilter<>(100, 0.01);
        String element1 = "apple";
        String element2 = "banana";
        String element3 = "cherry";

        bf.add(element1);
        bf.add(element2);

        // The added elements should be reported as present.
        assertTrue(bf.contains(element1), "Bloom filter should contain 'apple'");
        assertTrue(bf.contains(element2), "Bloom filter should contain 'banana'");
        // 'cherry' was not added and is unlikely to be a false positive with these parameters
        assertFalse(bf.contains(element3), "Bloom filter should not contain 'cherry'");
    }

    @Test
    public void testFalsePositiveRate() {
        int n = 1000;
        double p = 0.01;  // Desired false positive probability of 1%
        BloomFilter<String> bf = new BloomFilter<>(n, p);

        // Add n elements.
        for (int i = 0; i < n; i++) {
            bf.add("element_" + i);
        }
        // Test membership for a separate set of elements.
        int falsePositives = 0;
        int trials = 1000;
        for (int i = n; i < n + trials; i++) {
            if (bf.contains("element_" + i)) {
                falsePositives++;
            }
        }
        double observedRate = (double) falsePositives / trials;
        // While the theoretical rate is 1%, allow some margin (e.g., <5%) due to randomness.
        assertTrue(observedRate < 0.05, "Observed false positive rate should be reasonably low: " + observedRate);
    }

    @Test
    public void testBitsetSizeAndNumHashFunctions() {
        int expectedElements = 100;
        double falsePositiveProbability = 0.01;
        BloomFilter<String> bf = new BloomFilter<>(expectedElements, falsePositiveProbability);

        // Compute expected bitset size: m = ceil(-n * ln(p) / (ln2)^2)
        int expectedBitsetSize = (int) Math.ceil(-expectedElements * Math.log(falsePositiveProbability)
                / Math.pow(Math.log(2), 2));
        // Compute expected number of hash functions: k = ceil((m / n) * ln2)
        int expectedNumHashFunctions = (int) Math.ceil((expectedBitsetSize / (double) expectedElements) * Math.log(2));

        assertEquals(expectedBitsetSize, bf.getBitsetSize(), "Bitset size should match the computed value");
        assertEquals(expectedNumHashFunctions, bf.getNumHashFunctions(), "Number of hash functions should match the computed value");
    }

    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        BloomFilter<String> bf = new BloomFilter<>(100, 0.01);
        bf.add("test");
        bf.add("serialize");

        // Serialize the Bloom filter to a byte array.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(bf);
        }
        byte[] serializedData = baos.toByteArray();

        // Deserialize the Bloom filter from the byte array.
        BloomFilter<String> deserializedBf;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(serializedData))) {
            deserializedBf = (BloomFilter<String>) ois.readObject();
        }
        // Verify that the deserialized filter contains the same elements.
        assertTrue(deserializedBf.contains("test"), "Deserialized Bloom filter should contain 'test'");
        assertTrue(deserializedBf.contains("serialize"), "Deserialized Bloom filter should contain 'serialize'");
        // Check that an element not added is not falsely reported as present.
        assertFalse(deserializedBf.contains("other"), "Deserialized Bloom filter should not contain 'other'");
    }
}
