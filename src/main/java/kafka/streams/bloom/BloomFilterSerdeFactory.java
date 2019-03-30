package kafka.streams.bloom;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class BloomFilterSerdeFactory {
  public static <T> Serde<BloomFilter<T>> createSerde(Function<T, Long> hashFunction) {
    BloomFilterSerializer<T> serializer = new BloomFilterSerializer<T>();
    serializer.configure(null, false);

    Map<String, Object> props = new HashMap<>();
    props.put(BloomFilterDeserializer.JSON_BLOOMFILTER_HASHFUNCTION_KEY, hashFunction);

    BloomFilterDeserializer<T> deserializer = new BloomFilterDeserializer<>();
    deserializer.configure(props, false);

    return Serdes.serdeFrom(serializer, deserializer);
  }
}
