# Purpose
This tool is a very hackish solution for exporting Kafka consumer groups' committed offsets from one Kafka cluster (source) to another one (destination).

It is meant to be run
- in the process of migrating consumers from the source to the destination Kafka clusters
- one-shot, after the target application has been stopped on the source cluster, and before it has been started on the destination cluster
- once per consumer group

# Usage

## Building the project
`mvn clean compile assembly:single`

## Preparing the config files
Create two property files with the necessary information to connect to the source (`source.cluster.properties`) and destination (`destination.cluster.properties`) clusters: two examples, with and without authentication are provided below:

```
# source.cluster.properties
bootstrap.servers=my-source-kafka-broker:9092
```

```
# destination.cluster.properties
bootstrap.servers=my-destination-kafka-broker:9092
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="my-username" password="my-password";
ssl.endpoint.identification.algorithm=https
```

## Running the one-shot replication
- stop the target application from the source cluster
- run the exporter:
`java -jar target/kafka-consumegroup-exporter-1.0-SNAPSHOT-jar-with-dependencies.jar my-consumer-group latest`
where:
- the first argument indicates the ID of the consumer group to export
- the second argument can be one of `earliest`, `latest`, `none`, and indicates the translator how to behave when the export of the committed offset fails for a partition:
  - `none` will skip the failed offset: when the consumer group starts consuming from the new cluster, it will reset its offset according to its own `auto.offset.reset` configuration
  - `latest` will automatically commit the latest offset available for the failed partition on the destination cluster
  - `latest` will automatically commit the earliest offset available for the failed partition on the destination cluster
- verify the committed offset in the destination cluster (i.e. `kafka-consumer-groups` command)
- start the app in the destination cluster
