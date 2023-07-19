package io.surisoft.websocket.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerRecordProcessor<K, V> {
    void process(ConsumerRecord<K, V> consumerRecord);
}