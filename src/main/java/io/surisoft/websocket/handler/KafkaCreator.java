package io.surisoft.websocket.handler;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Properties;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@Component
public class KafkaCreator {
    @Autowired
    KafkaProperties kafkaProperties;
    @Autowired
    TaskExecutor kafkaTaskExecutor;
    @Value("${tester.kafka.topic}")
    private String kafkaTopic;

    protected Function<Properties, KafkaConsumer<String, String>> kafkaConsumerFactory = KafkaConsumer::new;

    private Properties getProperties() {
        Properties properties = new Properties();
        properties.putAll(kafkaProperties.buildConsumerProperties());
        properties.put(GROUP_ID_CONFIG, this.kafkaTopic + "-notification-consumer");
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE);
        return properties;
    }

    public KafkaConsumer<String, String> createConsumer(String clientId) {
        KafkaConsumer<String, String> consumer = kafkaConsumerFactory.apply(getProperties());
        TopicPartition clientPartition = partition(consumer, clientId);
        consumer.assign(singletonList(clientPartition));
        consumer.seekToEnd(singletonList(clientPartition));
        consumer.position(clientPartition);
        return consumer;
    }

    public TopicPartition partition(Consumer<String, String> consumer, String clientId) {
        List<PartitionInfo> partitions = consumer.partitionsFor(this.kafkaTopic);
        int numPartitions = partitions.size();
        int partition = Utils.toPositive(Utils.murmur2(clientId.getBytes())) % numPartitions;
        return new TopicPartition(this.kafkaTopic, partition);
    }

    public TaskExecutor getTaskExecutor() {
        return kafkaTaskExecutor;
    }
}