package kafka.streams.bloom;

// see also:
// https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions
// https://hur.st/bloomfilter
public final class BloomFilterUtil {

  private static double LOG2 = Math.log(2);
  private static double SQUARE_OF_LOG2 = LOG2 * LOG2;

  public static int getOptimalNbHashFunctions(long expectedCount, long numBits) {
    return Math.max(1, (int) Math.round(LOG2 * numBits / expectedCount));
  }

  public static int getOptimalNbBits(long expectedCount, double falsePositiveProbability) {
    return (int) (-expectedCount * Math.log(falsePositiveProbability) / SQUARE_OF_LOG2);
  }

  private BloomFilterUtil() {
  }
}
