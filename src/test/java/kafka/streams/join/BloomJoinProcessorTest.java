package kafka.streams.join;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;

import org.apache.hive.common.util.Murmur3;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.MockProcessorContext.CapturedForward;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import kafka.streams.bloom.BloomFilter;
import kafka.streams.bloom.BloomFilterSerdeFactory;

public class BloomJoinProcessorTest {

  private MockProcessorContext processorContext;
  private BloomJoinProcessor<String, String> processor;

  private Function<String, Long> murmur3 = x -> Murmur3.hash64(x.getBytes(StandardCharsets.UTF_8));

  @BeforeEach
  void setUp() throws Exception {
    String storeName = "bloom-join-store" + UUID.randomUUID();
    Function<String, BloomFilter<String>> filterSupplier = streamId -> new BloomFilter<>(murmur3, 10000);
    processor = new BloomJoinProcessor<>((key, value) -> key, filterSupplier, (key, value) -> "output-stream",
        storeName);

    Properties config = new Properties();
    config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "bloom-join-processor-test");
    config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "nowhere:1234");
    config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    KeyValueStore<String, BloomFilter<String>> store = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore(storeName), Serdes.String(),
            BloomFilterSerdeFactory.createSerde(murmur3))
        .withLoggingDisabled().build();

    processorContext = new MockProcessorContext(config);
    store.init(processorContext, store);
    processorContext.register(store, null);
    processor.init(processorContext);
  }

  @Test
  void testJoinTwoItems() {
    processor.process("stream1", "value1");
    processor.process("stream2", "value1");

    List<CapturedForward> output = processorContext.forwarded();
    assertEquals(1, output.size());
    assertEquals("output-stream", output.get(0).keyValue().key);
    assertEquals("value1", output.get(0).keyValue().value);
  }

  @Test
  void testNotJoinTwoItems() {
    processor.process("stream1", "value1");
    processor.process("stream2", "value2");

    assertTrue(processorContext.forwarded().isEmpty());
  }

  @Test
  void testSimpleJoinCase() {
    List<CapturedForward> output = null;

    processor.process("stream1", "value1");
    processor.process("stream1", "value1");
    assertTrue(processorContext.forwarded().isEmpty());

    processor.process("stream2", "value2");
    processor.process("stream2", "value2");
    assertTrue(processorContext.forwarded().isEmpty());

    processor.process("stream1", "value2");
    output = processorContext.forwarded();
    assertEquals("output-stream", output.get(0).keyValue().key);
    assertEquals("value2", output.get(0).keyValue().value);

    processor.process("stream2", "value1");
    output = processorContext.forwarded();
    assertEquals("output-stream", output.get(1).keyValue().key);
    assertEquals("value1", output.get(1).keyValue().value);

    processor.process("stream1", "value2");
    output = processorContext.forwarded();
    assertEquals("output-stream", output.get(2).keyValue().key);
    assertEquals("value2", output.get(2).keyValue().value);

    processor.process("stream2", "value1");
    output = processorContext.forwarded();
    assertEquals("output-stream", output.get(3).keyValue().key);
    assertEquals("value1", output.get(3).keyValue().value);
  }
}
