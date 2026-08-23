package com.kafkaProject.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Demonstrates Kafka's exactly-once building blocks:
 *   1. enable.idempotence=true -> broker de-dupes retried sends from THIS producer
 *      (protects against network blips causing the same message to be written twice)
 *   2. transactional.id + beginTransaction/commitTransaction -> all sends in the
 *      transaction become visible atomically, or none do (used when a producer
 *      needs to write to multiple partitions/topics as a single atomic unit)
 *
 * This is the "true EOS" mechanism, as opposed to the more common real-world
 * pattern of at-least-once + idempotent DB writes (see OrderConsumer.java).
 * Interviewers like candidates who can name BOTH approaches and explain when
 * each is actually used.
 *
 * Run with:
 *   mvn exec:java -Dexec.mainClass="com.interviewprep.kafka.ExactlyOnceProducer"
 */
public class ExactlyOnceProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        // idempotent producer: safe retries, no duplicate writes from THIS producer
        props.put("enable.idempotence", true);
        props.put("acks", "all");

        // transactional id must be unique per producer instance
        props.put("transactional.id", "order-txn-producer-1");

        Producer<String, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();

        try {
            producer.beginTransaction();

            producer.send(new ProducerRecord<>(TopicAdmin.TOPIC, "SUB-2001",
                    "{\"subscriberId\":\"SUB-2001\",\"amount\":200,\"eventType\":\"RECHARGE\"}"));
            producer.send(new ProducerRecord<>(TopicAdmin.TOPIC, "SUB-2001",
                    "{\"subscriberId\":\"SUB-2001\",\"amount\":200,\"eventType\":\"AML_CHECK\"}"));

            // both messages above become visible to consumers together, atomically,
            // only once commitTransaction() succeeds
            producer.commitTransaction();
            System.out.println("Transaction committed - both events visible atomically.");

        } catch (Exception e) {
            System.out.println("Error occurred, aborting transaction: " + e.getMessage());
            producer.abortTransaction(); // neither message becomes visible
        } finally {
            producer.close();
        }
    }
}
