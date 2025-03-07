package kvStore.bloomFilter;

import java.io.Serial;
import java.io.Serializable;
import java.util.BitSet;

/*
BloomFilter Class:

The contains method checks whether all of the bits for the computed hash values are set.
 */
public class BloomFilter<T> implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private final BitSet bitset;
    private final int bitsetSize;
    private final int numHashFunctions;
    private final int expectedElements;
    private final double falsePositiveProbability;

    /**
     * Constructs a BloomFilter.
     * The constructor computes the optimal bitset size (m) and the number of hash functions (k) using standard formulas.
     * @param expectedElements Expected number of elements to be stored.
     * @param falsePositiveProbability Desired false positive probability.
     */
    public BloomFilter(int expectedElements, double falsePositiveProbability) {
        this.expectedElements = expectedElements;
        this.falsePositiveProbability = falsePositiveProbability;
        // Compute bitset size: m = -(n * ln(p)) / (ln2)^2
        // Source of equations: https://en.wikipedia.org/wiki/Bloom_filter
        this.bitsetSize = (int) Math.ceil(-expectedElements * Math.log(falsePositiveProbability) / (Math.pow(Math.log(2), 2)));
        // Compute number of hash functions: k = (m/n) * ln2
        this.numHashFunctions = (int) Math.ceil((bitsetSize / (double) expectedElements) * Math.log(2));
        this.bitset = new BitSet(bitsetSize);
    }

    /**
     * Adds an element to the Bloom filter.
     * The add method computes multiple hash values (using a simple double hashing strategy) and sets bits in the bitset.
     * @param element The element to add.
     */
    public void add(T element) {
        int[] hashes = createHashes(element, numHashFunctions);
        for (int hash : hashes) {
            int pos = Math.abs(hash % bitsetSize);
            bitset.set(pos);
        }
    }

    /**
     * Checks if the element might be in the Bloom filter.
     * @param element The element to check.
     * @return True if the element might be present; false if definitely not present.
     */
    public boolean contains(T element) {
        int[] hashes = createHashes(element, numHashFunctions);
        for (int hash : hashes) {
            int pos = Math.abs(hash % bitsetSize);
            if (!bitset.get(pos)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Creates multiple hash values for the given element.
     * This implementation uses double hashing.
     * @param element The element to hash.
     * @param numHashes The number of hash values to produce.
     * @return An array of hash values.
     */
    private int[] createHashes(T element, int numHashes) {
        int[] result = new int[numHashes];
        int hash1 = element.hashCode();
        int hash2 = hash1 >>> 16; // A simple way to generate a second hash.
        for (int i = 0; i < numHashes; i++) {
            result[i] = hash1 + i * hash2;
        }
        return result;
    }

    public int getBitsetSize() {
        return bitsetSize;
    }

    public int getNumHashFunctions() {
        return numHashFunctions;
    }
}
