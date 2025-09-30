package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import java.time.Duration;
import java.util.*;

public class KafkaOffsetFinder {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "offset-finder-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        String topic = "your-topic";
        String key = "your-key";
        long timestamp = 1696060000000L; // 찾고싶은 timestamp (ms)

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // 1. 파티션 정보 가져오기
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);

            // 키 → 파티션 매핑
            int numPartitions = partitions.size();
            int partition = Math.abs(key.hashCode()) % numPartitions;

            TopicPartition tp = new TopicPartition(topic, partition);
            consumer.assign(Collections.singleton(tp));

            // 2. 타임스탬프 → 오프셋 조회
            Map<TopicPartition, Long> query = new HashMap<>();
            query.put(tp, timestamp);
            Map<TopicPartition, OffsetAndTimestamp> result = consumer.offsetsForTimes(query);

            OffsetAndTimestamp oat = result.get(tp);
            if (oat == null) {
                System.out.println("해당 타임스탬프 이후 데이터 없음");
                return;
            }

            long offset = oat.offset();
            consumer.seek(tp, offset);

            // 3. 키 일치하는 레코드 찾기
            boolean found = false;
            while (!found) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) break;

                for (ConsumerRecord<String, String> record : records) {
                    if (key.equals(record.key())) {
                        System.out.printf("Found key=%s at offset=%d timestamp=%d value=%s%n",
                                record.key(), record.offset(), record.timestamp(), record.value());
                        found = true;
                        break;
                    }
                }
            }

            if (!found) {
                System.out.println("해당 키 데이터 없음");
            }
        }
    }
}
