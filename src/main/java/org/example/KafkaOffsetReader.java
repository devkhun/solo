package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaOffsetReader {
    public static void main(String[] args) {
        String topic = "my-topic";
        int partition = 0;
        long startOffset = 100L;  // 원하는 offset

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "offset-reader-group");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);

            // 특정 파티션만 구독
            consumer.assign(Collections.singletonList(topicPartition));

            // 원하는 offset으로 이동
            consumer.seek(topicPartition, startOffset);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partition=%d, offset=%d, key=%s, value=%s%n",
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value());
                }
            }
        }
    }
}

