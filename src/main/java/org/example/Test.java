package org.example;

public class Test {
    private static final int THREAD_COUNT = 10;
    private List<EventQueue> queues = new ArrayList<>();
    private List<Thread> workers = new ArrayList<>();
    private Subscriber sub;

    protected void init() {
        for (int i = 0; i < THREAD_COUNT; i++) {
            EventQueue q = ftl.getRealm().createEventQueue();
            queues.add(q);

            Thread t = new Thread(() -> {
                while (true) {
                    q.dispatch(1); // 큐별로 독립 dispatch loop
                }
            }, "worker-" + i);
            t.start();
            workers.add(t);
        }

        sub = ftl.getRealm().createSubscriber(endpoint, cm, props);





        queue = ftl.getRealm().createEventQueue();
        executors = new ExecutorService[10];
        for (int i = 0; i < executors.length; i++) {
            executors[i] = Executors.newSingleThreadExecutor();
        }

        sub = ftl.getRealm().createSubscriber(endpoint, cm, props);

        new Thread(() -> {
            while (true) {
                queue.dispatch(1);
            }
        }).start();
    }

    private void subscribe() {
        // 모든 큐에 동일 Subscriber를 등록하되, 콜백에서 키 해시로 큐 분배
        for (EventQueue q : queues) {
            q.addSubscriber(sub, (msgs, qrecv) -> {
                for (Message msg : msgs) {
                    String key = msg.get("key"); // 예시
                    int idx = Math.abs(key.hashCode()) % THREAD_COUNT;
                    EventQueue targetQueue = queues.get(idx);
                    targetQueue.enqueue(() -> processMessage(msg)); // 표준화 + Kafka 발행
                }
            });
        }


        queue.addSubscriber(sub, (msgs, qrecv) -> {
            for (Message msg : msgs) {
                String key = msg.get("key");
                int idx = Math.abs(key.hashCode()) % executors.length;
                executors[idx].submit(() -> processMessage(msg));
            }
        });
    }

    private void processMessage(Message msg) {
        // 데이터 표준화 후 Kafka 발행
    }


}
