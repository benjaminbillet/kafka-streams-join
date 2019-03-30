# Kafka Streams Join Processor

The purpose of this [Kafka Streams processor](https://kafka.apache.org/21/documentation/streams/developer-guide/processor-api.html) is to perform a continuous [inner-join](https://www.dofactory.com/sql/join) over a set of streams. Basically, the processor accumulates all unique record value of the streams. When a new record is processed, the processor looks if the record value has been seen so far on all other streams. If so, the processor forward the record as part of the joined stream.
