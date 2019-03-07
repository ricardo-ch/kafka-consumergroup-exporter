package ch.ricardo.kafka.consumergroupexporter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;

public class App {

  private static final Logger log = LoggerFactory.getLogger(App.class);

  private static final Properties sourceClusterProperties = new Properties();
  private static final Properties destinationClusterProperties = new Properties();

  public static void main(String[] args) throws Exception {

    assert args.length == 2;

    // load cluster settings from config files
    InputStream destinationPropsStream = new FileInputStream("destination.cluster.properties");
    InputStream sourcePropsStream = new FileInputStream("source.cluster.properties");

    destinationClusterProperties.load(destinationPropsStream);
    sourceClusterProperties.load(sourcePropsStream);

    // do the magic
    exportConsumerGroup(args[0], OffsetResetStrategy.valueOf(args[1].toUpperCase()));
  }

  private static void exportConsumerGroup(String groupId, OffsetResetStrategy fallbackStrategy) throws Exception {

    log.info("Exporting consumer group " + groupId
        + " from " + sourceClusterProperties.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG)
        + " to " + destinationClusterProperties.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));

    AdminClient client = KafkaAdminClient.create(sourceClusterProperties);

    Properties sourceClusterConsumerProperties = new Properties();
    sourceClusterConsumerProperties.putAll(sourceClusterProperties);
    sourceClusterConsumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
    sourceClusterConsumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
    KafkaConsumer dataCenterConsumer = new KafkaConsumer(sourceClusterConsumerProperties);


    Properties destinationClusterConsumerProperties = new Properties();
    destinationClusterConsumerProperties.putAll(destinationClusterProperties);
    destinationClusterConsumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    destinationClusterConsumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
    destinationClusterConsumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    destinationClusterConsumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
    destinationClusterConsumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
    KafkaConsumer cloudConsumer = new KafkaConsumer(destinationClusterConsumerProperties);

    Map<TopicPartition, OffsetAndMetadata> partitions =
        client.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();

    for (TopicPartition tp : partitions.keySet()) {
      // get the offset in the source cluster
      long offset = partitions.get(tp).offset();
      log.info(tp.toString() + " committed offset on source cluster is " + offset);

      // get the timestamp associated to the offset
      dataCenterConsumer.assign(ImmutableList.of(tp));
      dataCenterConsumer.seek(tp,  Math.max(offset - 1, 0));

      ConsumerRecords consumerRecords = dataCenterConsumer.poll(Duration.ofSeconds(1));
      long timestamp = 0;

      if (consumerRecords.isEmpty()) {
        log.warn(tp.toString() + ": no message could be consumed from source cluster");
      } else {
        ConsumerRecord record = (ConsumerRecord) consumerRecords.iterator().next();

        timestamp = record.timestamp();

        log.info(tp.toString() + ": maps to timestamp " + timestamp);
      }

      if (timestamp > 0) {
        Map<TopicPartition, OffsetAndTimestamp> remoteOffset = cloudConsumer.offsetsForTimes(ImmutableMap.of(tp, timestamp));

        if (remoteOffset != null && remoteOffset.get(tp) != null) {
          long remoteOffsetValue = remoteOffset.get(tp).offset() + 1L;

          log.info(tp.toString() + ": translated to offset " + remoteOffsetValue + " on destination cluster");

          cloudConsumer.commitSync(ImmutableMap.of(tp, new OffsetAndMetadata(remoteOffsetValue)));
          continue;
        }
        log.warn(tp.toString() + ": remote offset not found");
      } else {
        log.warn(tp.toString() + ": local timestamp not available");
      }

      switch (fallbackStrategy) {
        case EARLIEST:
          log.warn(tp.toString() + ": falling back - seeking to beginning");
          cloudConsumer.commitSync(ImmutableMap.of(tp, new OffsetAndMetadata(1L)));
          break;
        case LATEST:
          log.warn(tp.toString() + ": falling back - seeking to end");
          Map<TopicPartition, Long> endOffsets = cloudConsumer.endOffsets(ImmutableList.of(tp));
          cloudConsumer.commitSync(ImmutableMap.of(tp, new OffsetAndMetadata(endOffsets.get(tp))));
          break;
        default:
          log.warn(tp.toString() + ": no offset is committed in the destination cluster - the app will follow its configured strategy");
      }
    }
  }
}
