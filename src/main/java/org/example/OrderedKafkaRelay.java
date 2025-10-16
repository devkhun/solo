package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class OrderedKafkaRelay {

    private static final String SOURCE_TOPIC = "source-topic";
    private static final String TARGET_TOPIC = "target-topic";
    private static final Properties consumerProps = new Properties();
    private static final Properties producerProps = new Properties();

    static {
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "ordered-relay-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    }

    static class Message {
        String uniqueId;   // EQP_ID or LOT_ID
        long timestamp;    // YYMMDDHHIISS -> long 변환
        String payload;

        Message(String uniqueId, long timestamp, String payload) {
            this.uniqueId = uniqueId;
            this.timestamp = timestamp;
            this.payload = payload;
        }
    }

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        ExecutorService executor = Executors.newFixedThreadPool(4);

        consumer.subscribe(Collections.singletonList(SOURCE_TOPIC));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) continue;

                // 메시지 변환
                List<Message> messages = new ArrayList<>();
                for (ConsumerRecord<String, String> r : records) {
                    String[] parts = r.value().split(",");
                    // 예: "EQP_ID=2025101601,20251016010200,payload"
                    String id = parts[0].split("=")[1];
                    long ts = Long.parseLong(parts[1]);
                    String payload = parts[2];
                    messages.add(new Message(id, ts, payload));
                }

                // 그룹화 및 병렬 처리
                Map<String, List<Message>> grouped =
                        messages.stream().collect(Collectors.groupingBy(m -> m.uniqueId));

                for (var entry : grouped.entrySet()) {
                    executor.submit(() -> {
                        List<Message> ordered = entry.getValue().stream()
                                .sorted(Comparator.comparingLong(m -> m.timestamp))
                                .toList();

                        for (Message msg : ordered) {
                            ProducerRecord<String, String> record =
                                    new ProducerRecord<>(TARGET_TOPIC, msg.uniqueId, msg.payload);
                            producer.send(record, (metadata, ex) -> {
                                if (ex != null) ex.printStackTrace();
                            });
                        }
                    });
                }
            }
        } finally {
            consumer.close();
            producer.close();
            executor.shutdown();
        }
    }
}
