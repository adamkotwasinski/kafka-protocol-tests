# kafka-protocol-tests

Some tests to be executed while developing Kafka extensions for Envoy.

Should be merged into Envoy build process, but that would require considerable (I think) Bazel knowledge and effort.

Dependencies:
* Kafka server has to be listening on 9092 and advertise itself on 19092,
* Envoy has to listen on 19092 and forward to Kafka's 9092,
* Envoy exposes metrics on 9901,
* tests should be executed against clean installation (we don't want to have any records in topics, as they might break tests).
