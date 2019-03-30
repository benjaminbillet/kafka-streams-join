# Kafka Streams Join Processor

The purpose of this [Kafka Streams processor](https://kafka.apache.org/21/documentation/streams/developer-guide/processor-api.html) is to perform a continuous [inner-join](https://www.dofactory.com/sql/join) over a set of streams. Basically, the processor accumulates all unique record value of the streams. When a new record is processed, the processor looks if the record value has been seen so far on all other streams. If so, the processor forward the record as part of the joined stream.

This project includes two implementation:
- `HashJoinProcessor`: deterministic implementation, based on hash tables (more precisely, Java `HashSet`).
- `BloomJoinProcessor`: probabilistic implementation, based on [Bloom filters](https://en.wikipedia.org/wiki/Bloom_filter). This implementation consumes order of magnitude less memory, but has a probability of false positive (i.e., a few items in the result streams should have not been part of the join).
