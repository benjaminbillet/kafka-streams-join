package kafka.streams.join;

import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import kafka.streams.bloom.BloomFilter;

/**
 * A processor that implements an approximated inner-join for data streams
 * values. The implementation is based on bloom filter (for each streams, a
 * filter is used for "storing" elements).
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class BloomJoinProcessor<K, V> implements Processor<K, V> {
  private ProcessorContext context;
  private String storeName;
  private KeyValueStore<String, BloomFilter<V>> store;

  private final int nbStreams;
  private final BiFunction<K, V, String> streamIdExtractor;
  private final BiFunction<K, V, K> outputKeyMapper;
  private final Function<String, BloomFilter<V>> bloomFilterSupplier;

  /**
   * @param streamIdExtractor   A function that can extracts a stream identifier
   *                            from a stream record.
   * @param bloomFilterSupplier A supplier function for configured bloom filters.
   * @param outputKeyMapper     A function that supplies keys for the output
   *                            records.
   * @param storeName           The name of the store used to save the content of
   *                            the hash tables.
   */
  public BloomJoinProcessor(BiFunction<K, V, String> streamIdExtractor,
      Function<String, BloomFilter<V>> bloomFilterSupplier, BiFunction<K, V, K> outputKeyMapper, String storeName) {
    this(2, streamIdExtractor, bloomFilterSupplier, outputKeyMapper, storeName);
  }

  /**
   * @param nbStreams           The (minimum) number of streams to join.
   * @param streamIdExtractor   A function that can extracts a stream identifier
   *                            from a stream record.
   * @param bloomFilterSupplier A supplier function for configured bloom filters.
   * @param outputKeyMapper     A function that supplies keys for the output
   *                            records.
   * @param storeName           The name of the store used to save the content of
   *                            the hash tables.
   */
  public BloomJoinProcessor(int nbStreams, BiFunction<K, V, String> streamIdExtractor,
      Function<String, BloomFilter<V>> bloomFilterSupplier, BiFunction<K, V, K> outputKeyMapper, String storeName) {
    this.nbStreams = nbStreams;
    this.bloomFilterSupplier = bloomFilterSupplier;
    this.streamIdExtractor = streamIdExtractor;
    this.outputKeyMapper = outputKeyMapper;
    this.storeName = storeName;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    this.store = (KeyValueStore<String, BloomFilter<V>>) context.getStateStore(storeName);

    Gauge.builder("store-size", this.store, store -> store.approximateNumEntries()).tag("store-name", storeName)
        .description("Approximate number of objects into the store").register(Metrics.globalRegistry);
  }

  @Override
  public void process(K key, V value) {
    // if the hash table does not exists for the stream, we create it
    String sourceStream = streamIdExtractor.apply(key, value);
    BloomFilter<V> filter = store.get(sourceStream);
    if (filter == null) {
      filter = bloomFilterSupplier.apply(sourceStream);
    }

    // we look for the value in all other streams
    int matched = 0;
    KeyValueIterator<String, BloomFilter<V>> it = store.all();
    while (it.hasNext()) {
      KeyValue<String, BloomFilter<V>> kv = it.next();
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

    filter.add(value);
    store.put(sourceStream, filter);
  }

  @Override
  public void close() {
    // nothing to do
  }
}
