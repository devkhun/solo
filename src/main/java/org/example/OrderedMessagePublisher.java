package org.example;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class OrderedMessagePublisher {

    private final KafkaPublisher kafkaPublisher;
    // 유니크아이디별 버퍼
    private final Map<String, List<Message>> messageBuffer = new ConcurrentHashMap<>();

    public OrderedMessagePublisher(KafkaPublisher kafkaPublisher) {
        this.kafkaPublisher = kafkaPublisher;
    }

    // 메시지 구조
    public static class Message {
        private final String id;      // 예: A, B, C
        private final String stage;   // 예: START, MAINT, END
        private final String payload;

        public Message(String id, String stage, String payload) {
            this.id = id;
            this.stage = stage;
            this.payload = payload;
        }

        public String getId() { return id; }
        public String getStage() { return stage; }
        public String getPayload() { return payload; }
    }

    // 메시지 처리 로직
    public void handleMessage(Message msg) {
        // 3쌍이 필요한 ID만 버퍼링
        if (requiresOrdering(msg.getId())) {
            messageBuffer.computeIfAbsent(msg.getId(), k -> new ArrayList<>()).add(msg);
            flushIfComplete(msg.getId());
        } else {
            kafkaPublisher.publish(msg.getPayload());
        }
    }

    // 완성된 ID(3단계 모두 도착)면 정렬 후 발행
    private void flushIfComplete(String id) {
        List<Message> list = messageBuffer.get(id);
        if (list == null) return;

        // START, MAINT, END 모두 도착한 경우에만
        Set<String> stages = new HashSet<>();
        for (Message m : list) stages.add(m.getStage());

        if (stages.containsAll(Arrays.asList("START", "MAINT", "END"))) {
            // 정렬 후 발행
            list.sort(Comparator.comparingInt(m -> stageOrder(m.getStage())));
            for (Message m : list) {
                kafkaPublisher.publish(m.getPayload());
            }
            messageBuffer.remove(id);
        }
    }

    // 순서 정의
    private int stageOrder(String stage) {
        switch (stage) {
            case "START": return 1;
            case "MAINT": return 2;
            case "END": return 3;
            default: return 999;
        }
    }

    // 순서 보장 필요한 ID 판별
    private boolean requiresOrdering(String id) {
        // 여기서는 예시로 A, C만 정렬 대상으로 설정
        return id.equals("A") || id.equals("C");
    }
}


/*
사용예시
OrderedMessagePublisher publisher = new OrderedMessagePublisher(new KafkaPublisher());

// 입력 순서 (무작위 도착)
publisher.handleMessage(new Message("A", "END", "A-END"));
publisher.handleMessage(new Message("A", "START", "A-START"));
publisher.handleMessage(new Message("B", "1", "B-1"));
publisher.handleMessage(new Message("C", "MAINT", "C-MAINT"));
publisher.handleMessage(new Message("C", "START", "C-START"));
publisher.handleMessage(new Message("A", "MAINT", "A-MAINT"));
publisher.handleMessage(new Message("C", "END", "C-END"));
publisher.handleMessage(new Message("E", "1", "E-1"));

 */
