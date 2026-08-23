package com.kafkaProject.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Creates the "orders" topic used by every other demo class.
 *
 * Run with:
 *   mvn exec:java -Dexec.mainClass="com.interviewprep.kafka.TopicAdmin"
 *
 * Try changing PARTITIONS and REPLICATION_FACTOR and re-running to see the
 * effect in Kafka UI (http://localhost:8080) - watch how partitions spread
 * across brokers and which broker becomes "leader" for each partition.
 */

public class TopicAdmin {

    static final String TOPIC = "Zebseron";
    static final int PARTITIONS = 4;          // try bumping to 6 -> more parallelism
    // Single local broker -> replication factor MUST be 1 (can't replicate to brokers
    // that don't exist). In a real cluster you'd set this to 3. You can still explain
    // leader/follower/ISR/failover conceptually in interviews using the diagram from
    // your notes -- you just can't physically trigger a failover on one broker.
    static final short REPLICATION_FACTOR = 1;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        try (AdminClient admin = AdminClient.create(props)) {
            NewTopic topic = new NewTopic(TOPIC, PARTITIONS, REPLICATION_FACTOR);
            admin.createTopics(Collections.singleton(topic)).all().get();
            System.out.printf("Created topic '%s' with %d partitions, replication factor %d%n",
                    TOPIC, PARTITIONS, REPLICATION_FACTOR);
        } catch (ExecutionException e) {
            if (e.getCause() != null && e.getCause().getMessage().contains("already exists")) {
                System.out.println("Topic already exists - that's fine, continuing.");
            } else {
                throw e;
            }
        }
    }
}
