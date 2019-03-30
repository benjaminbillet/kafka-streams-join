package kafka.streams.bloom;

import java.util.BitSet;
import java.util.Objects;
import java.util.function.Function;

import javax.validation.constraints.NotNull;

/**
 * A bloom filter is a probabilistic compact data structure for checking if an item is a member of a set.
 * The guarantees of a bloom filter are:
 * - if the bloom filter says that the element IS NOT IN the set, it is always true
 * - if the bloom filter says that the element IS IN the set, the element may not be in the set (probability of false positive)
 * See http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html for more information regarding the false positive probability.
 * @param <T> The type of items stored by the filter.
 */
public class BloomFilter<T> {
  private final Function<T, Long> hashFunction;
  private final int nbBits;
  private final int nbHashFunctions;
  private final BitSet bitset;

  private int count;

  public BloomFilter(@NotNull Function<T, Long> hashFunction, long expectedCount) {
    this(hashFunction, expectedCount, 0.05);
  }

  /**
   * Create a new bloom filter. The quality (i.e., the probability of false positives) depends on the number of items stored by the filter.
   * If ther number of item is known, it is possible to find the optimal size of the filter for a given false positive probability.
   * @param hashFunction A function for hashing items into long integers.
   * @param expectedCount The number of item expected in the filter.
   * @param falsePositiveProbability The expected false positive probability.
   */
  public BloomFilter(@NotNull Function<T, Long> hashFunction, long expectedCount, double falsePositiveProbability) {
    Objects.requireNonNull(hashFunction);
    if (expectedCount <= 0) {
      throw new IllegalArgumentException("expectedCount should be strictly positive");
    }
    if (falsePositiveProbability <= 0.0 || falsePositiveProbability >= 1.0) {
      throw new IllegalArgumentException("falsePositiveProbability should be in [0, 1]");
    }

    int optimal = BloomFilterUtil.getOptimalNbBits(expectedCount, falsePositiveProbability);
    this.nbBits = packTo64bitsWord(optimal);
    this.nbHashFunctions = BloomFilterUtil.getOptimalNbHashFunctions(expectedCount, nbBits);
    this.bitset = new BitSet(nbBits);
    this.hashFunction = hashFunction;
  }

  /**
   * Create a new bloom filter, with a fixed size and a fixed number of hash functions.
   * @param hashFunction A function for hashing items into long integers.
   * @param nbBits
   * @param nbHashFunctions
   */
  public BloomFilter(@NotNull Function<T, Long> hashFunction, int nbBits, int nbHashFunctions) {
    this(hashFunction, new BitSet(packTo64bitsWord(nbBits)), packTo64bitsWord(nbBits), nbHashFunctions, 0);
  }

  BloomFilter(@NotNull Function<T, Long> hashFunction, BitSet bitset, int nbBits, int nbHashFunctions, int count) {
    Objects.requireNonNull(hashFunction);
    if (nbBits <= 0) {
      throw new IllegalArgumentException("nbBits should be strictly positive");
    }
    if (nbHashFunctions <= 0) {
      throw new IllegalArgumentException("nbHashFunctions should be strictly positive");
    }

    this.count = count;
    this.nbBits = nbBits;
    this.nbHashFunctions = nbHashFunctions;
    this.bitset = bitset;
    this.hashFunction = hashFunction;
  }

  /**
   * Add a new item to the filter.
   * @param item
   */
  public void add(@NotNull T item) {
    Objects.requireNonNull(item);
    addHash(this.hashFunction.apply(item));
    count++;
  }

  private void addHash(long hash) {
    int hash1 = (int) hash;
    int hash2 = (int) (hash >>> 32);

    for (int i = 1; i <= nbHashFunctions; i++) {
      int combinedHash = hash1 + ((i + 1) * hash2);
      if (combinedHash < 0) {
        combinedHash = ~combinedHash; // flip if negative
      }
      int pos = combinedHash % nbBits;
      bitset.set(pos);
    }
  }

  /**
   * Test if an item exists in the filter.
   * @param item
   * @return
   */
  public boolean contains(@NotNull T item) {
    Objects.requireNonNull(item);
    return containsHash(this.hashFunction.apply(item));
  }

  private boolean containsHash(long hash64) {
    int hash1 = (int) hash64;
    int hash2 = (int) (hash64 >>> 32);

    for (int i = 1; i <= nbHashFunctions; i++) {
      int combinedHash = hash1 + ((i + 1) * hash2);
      if (combinedHash < 0) {
        combinedHash = ~combinedHash; // flip if negative
      }
      int pos = combinedHash % nbBits;
      if (bitset.get(pos) == false) {
        return false;
      }
    }
    return true;
  }

  public int getNbBits() {
    return nbBits;
  }

  public int getNbHashFunctions() {
    return nbHashFunctions;
  }

  public long[] getBitset() {
    return bitset.toLongArray();
  }

  public byte[] getBitsetBytes() {
    return bitset.toByteArray();
  }

  public int getCount() {
    return this.count;
  }

  /**
   * Clear the content of the filter.
   */
  public void clear() {
    bitset.clear();
    count = 0;
  }

  private static int packTo64bitsWord(int value) {
    return value + (Long.SIZE - (value % Long.SIZE));
  }
}
