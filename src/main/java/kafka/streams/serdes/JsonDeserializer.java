package kafka.streams.serdes;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonDeserializer<T> implements Deserializer<T> {
  public static final String JSON_DESERIALIZER_CLASS_KEY = "json.deserializer.class";

  private final ObjectMapper objectMapper = new ObjectMapper();
  private Class<T> clazz;

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
    this.clazz = (Class<T>) props.get(JSON_DESERIALIZER_CLASS_KEY);
  }

  @Override
  public T deserialize(String topic, byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    try {
      return objectMapper.readValue(bytes, clazz);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public void close() {
    // nothing to close
  }
}
