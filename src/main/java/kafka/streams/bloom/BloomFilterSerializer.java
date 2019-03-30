package kafka.streams.bloom;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class BloomFilterSerializer<T> implements Serializer<BloomFilter<T>> {
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
    // nothing to configure
  }

  @Override
  public byte[] serialize(String topic, BloomFilter<T> filter) {
    if (filter == null) {
      return null;
    }
    try {
      return objectMapper.writeValueAsBytes(
          new BloomFilterJson(filter.getBitsetBytes(), filter.getNbHashFunctions(), filter.getNbBits(), filter.getCount()));
    } catch (Exception e) {
      throw new SerializationException("Error serializing JSON message", e);
    }
  }

  @Override
  public void close() {
    // nothing to close
  }
}
