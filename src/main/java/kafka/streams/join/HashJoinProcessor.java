package kafka.streams.join;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;

/**
 * A processor that implements an exact inner-join for data streams values.
 * The implementation is based on hash tables (for each streams, a table is used to store all read elements).
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class HashJoinProcessor<K, V> implements Processor<K, V> {
  private ProcessorContext context;
  private String storeName;
  private KeyValueStore<String, Set<V>> store;

  private final int nbStreams;
  private final BiFunction<K, V, String> streamIdExtractor;
  private final BiFunction<K, V, K> outputKeyMapper;

  /**
   * @param streamIdExtractor A function that can extracts a stream identifier from a stream record.
   * @param outputKeyMapper A function that supplies keys for the output records.
   * @param storeName The name of the store used to save the content of the hash tables.
   */
  public HashJoinProcessor(BiFunction<K, V, String> streamIdExtractor, BiFunction<K, V, K> outputKeyMapper, String storeName) {
    this(2, streamIdExtractor, outputKeyMapper, storeName);
  }

  /**
   * @param nbStreams The (minimum) number of streams to join.
   * @param streamIdExtractor A function that can extracts a stream identifier from a stream record.
   * @param outputKeyMapper A function that supplies keys for the output records.
   * @param storeName The name of the store used to save the content of the hash tables.
   */
  public HashJoinProcessor(int nbStreams, BiFunction<K, V, String> streamIdExtractor, BiFunction<K, V, K> outputKeyMapper, String storeName) {
    this.nbStreams = nbStreams;
    this.streamIdExtractor = streamIdExtractor;
    this.outputKeyMapper = outputKeyMapper;
    this.storeName = storeName;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    this.store = (KeyValueStore<String, Set<V>>) context.getStateStore(storeName);

    Gauge.builder("store-size", this.store, store -> store.approximateNumEntries())
      .tag("store-name", storeName)
      .description("Approximate number of objects into the store")
      .register(Metrics.globalRegistry);
  }

  @Override
  public void process(K key, V value) {
    // if the hash table does not exists for the stream, we create it
    String sourceStream = streamIdExtractor.apply(key, value);
    Set<V> items = store.get(sourceStream);
    if (items == null) {
      items = new HashSet<>();
    }

    // we look for the value in all other streams
    int matched = 0;
    KeyValueIterator<String, Set<V>> it = store.all();
    while (it.hasNext()) {
      KeyValue<String, Set<V>> kv = it.next();
      if (sourceStream.equals(kv.key) == false) {
        if (kv.value.contains(value)) {
          matched++;
          if (matched >= nbStreams - 1) {
            break;
          }
        }
      }
    }

    // if found for at least nbStreams-1 streams, we forward the value
    if (matched >= nbStreams - 1) {
      K newKey = outputKeyMapper.apply(key, value);
      context.forward(newKey, value);
    }

    items.add(value);
    store.put(sourceStream, items);
  }

  @Override
  public void close() {
    // nothing to do
  }
}
