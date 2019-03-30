package kafka.streams.bloom;

public class BloomFilterJson {
  private byte[] bitset;
  private int count;
  private int nbHashFunctions;
  private int nbBits;

  public BloomFilterJson() {
    // for jackson
  }

  public BloomFilterJson(byte[] bitset, int nbHashFunctions, int nbBits, int count) {
    this.bitset = bitset;
    this.nbHashFunctions = nbHashFunctions;
    this.nbBits = nbBits;
    this.count = count;
  }

  public byte[] getBitset() {
    return this.bitset;
  }

  public void setBitset(byte[] bitset) {
    this.bitset = bitset;
  }

  public int getNbBits() {
    return this.nbBits;
  }

  public void setNbBits(int nbBits) {
    this.nbBits = nbBits;
  }

  public int getNbHashFunctions() {
    return this.nbHashFunctions;
  }

  public void setNbHashFunctions(int nbHashFunctions) {
    this.nbHashFunctions = nbHashFunctions;
  }

  public int getCount() {
    return this.count;
  }

  public void setCount(int count) {
    this.count = count;
  }
}
