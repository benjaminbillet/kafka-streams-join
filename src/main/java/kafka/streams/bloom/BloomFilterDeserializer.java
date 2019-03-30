package kafka.streams.bloom;

import java.util.BitSet;
import java.util.Map;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class BloomFilterDeserializer<T> implements Deserializer<BloomFilter<T>> {
  public static final String JSON_BLOOMFILTER_HASHFUNCTION_KEY = "json.bloomfilter.hashfunction";

  private final ObjectMapper objectMapper = new ObjectMapper();
  private Function<T, Long> hashFunction;

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
    hashFunction = (Function<T, Long>) props.get(JSON_BLOOMFILTER_HASHFUNCTION_KEY);
  }

  @Override
  public BloomFilter<T> deserialize(String topic, byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    try {
      BloomFilterJson data = objectMapper.readValue(bytes, BloomFilterJson.class);
      return new BloomFilter<>(hashFunction, BitSet.valueOf(data.getBitset()), data.getNbBits(),
          data.getNbHashFunctions(), data.getCount());
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public void close() {
    // nothing to close
  }
}
