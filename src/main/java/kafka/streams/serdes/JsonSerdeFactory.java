package kafka.streams.serdes;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerdeFactory {
  public static <T> Serde<T> createSerde(Class<T> clazz) {
    return createSerde(clazz, Collections.emptyMap());
  }

  public static <T> Serde<T> createSerde(Class<T> clazz, Map<String, Object> props) {
    Serializer<T> serializer = new JsonSerializer<>();
    serializer.configure(props, false);

    Map<String, Object> allProps = new HashMap<>(props);
    allProps.put(JsonDeserializer.JSON_DESERIALIZER_CLASS_KEY, clazz);

    Deserializer<T> deserializer = new JsonDeserializer<>();
    deserializer.configure(allProps, false);

    return Serdes.serdeFrom(serializer, deserializer);
  }
}
