package com.kafkaProject.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Publishes recharge/order events, keyed by subscriberId.
 *
 * WHAT TO OBSERVE:
 *  - Same subscriberId always lands on the same partition (check the printed
 *    partition number) -> that's how per-subscriber ordering is preserved.
 *  - Different subscriberIds spread across partitions -> that's your parallelism.
 *
 * Run with:
 *   mvn exec:java -Dexec.mainClass="com.interviewprep.kafka.OrderProducer" -Dexec.args="all"
 *   mvn exec:java -Dexec.mainClass="com.interviewprep.kafka.OrderProducer" -Dexec.args="1"
 *   mvn exec:java -Dexec.mainClass="com.interviewprep.kafka.OrderProducer" -Dexec.args="0"
 *
 * args[0] = acks setting: "all" (safest, waits for ISR), "1" (leader only), "0" (fire-and-forget)
 */
public class OrderProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String acks = args.length > 0 ? args[0] : "all";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        // ---- THE DURABILITY KNOB ----
        props.put("acks", acks);
        // acks=all is paired with min.insync.replicas (set on the topic/broker)
        // to guarantee the write survives even if the leader dies right after.
        if ("all".equals(acks)) {
            props.put("retries", 3);
            props.put("enable.idempotence", true); // avoids duplicate writes on retry
        }

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            String[] subscribers = {"SUB-1001", "SUB-1002", "SUB-1003", "SUB-1004", "SUB-1005","SUB-1006", "SUB-1007", "SUB-1006", "SUB-1005", "SUB-1004"};

            for (int i = 0; i < subscribers.length; i++) {
                String subscriberId = subscribers[i];
                String value = String.format(
                        "{\"subscriberId\":\"%s\",\"amount\":100,\"eventType\":\"RECHARGE\",\"seq\":%d}",
                        subscriberId, i);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(TopicAdmin.TOPIC, subscriberId, value);

                // send() is async; .get() here just blocks so we can print partition info clearly
                RecordMetadata metadata = producer.send(record).get();

                System.out.printf("Sent key=%s -> partition=%d offset=%d (acks=%s)%n",
                        subscriberId, metadata.partition(), metadata.offset(), acks);
            }
        }
    }
}
