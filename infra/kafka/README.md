# NowCPI Kafka

Kafka runs in KRaft mode and provides the event broker for asynchronous
NowCPI sources. Host-side producers connect to `localhost:9094`; services in
Compose use `kafka:9092`.

Current CPI extraction is still a file-based batch. The Kafka connector is
included in the Spark image for the later streaming ingestion step. Create
topics using the `nowcpi.*` namespace, such as `nowcpi.cpi`.
