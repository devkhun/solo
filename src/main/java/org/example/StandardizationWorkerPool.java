package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.*;

public class StandardizationWorkerPool {
    private static final int THREAD_COUNT = 10;
    private final List<BlockingQueue<String>> queues = new ArrayList<>();
    private final ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
    private final KafkaProducer<String, String> producer;

    public StandardizationWorkerPool(String bootstrapServers) {
        // Kafka 설정
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);

        // 각 스레드용 큐 초기화
        for (int i = 0; i < THREAD_COUNT; i++) {
            BlockingQueue<String> queue = new LinkedBlockingQueue<>();
            queues.add(queue);
            int threadId = i;
            executor.submit(() -> processQueue(threadId, queue));
        }
    }

    // Consistent hash 방식으로 데이터 → 특정 스레드 큐로 전달
    public void submitData(String dataId, String rawData) {
        int threadIndex = Math.abs(dataId.hashCode()) % THREAD_COUNT;
        queues.get(threadIndex).offer(rawData);
    }

    private void processQueue(int threadId, BlockingQueue<String> queue) {
        while (true) {
            try {
                String raw = queue.take();
                // 데이터 표준화 로직 (예시)
                String standardized = standardizeData(raw);

                // 표준화 완료 후 Kafka 발행
                producer.send(new ProducerRecord<>("standardized-topic", String.valueOf(threadId), standardized));
                System.out.printf("[Thread-%d] Published standardized data: %s%n", threadId, standardized);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private String standardizeData(String raw) {
        // 예시 표준화 로직
        return raw.trim().toUpperCase();
    }

    public void shutdown() {
        executor.shutdownNow();
        producer.close();
    }

    public static void main(String[] args) {
        StandardizationWorkerPool pool = new StandardizationWorkerPool("localhost:9092");

        // 더미 데이터 전송 예시
        for (int i = 0; i < 100; i++) {
            String dataId = String.valueOf(i);
            String rawData = "raw_data_" + i;
            pool.submitData(dataId, rawData);
        }
    }
}
