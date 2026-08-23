package com.kafkaProject.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Consumes recharge/order events and simulates a downstream DB write.
 *
 * WHAT TO OBSERVE:
 *  1. Consumer groups: run this TWICE in two terminals with the SAME group.id
 *     (e.g. both "balance-service") -> Kafka splits the 3 partitions between
 *     them. Then run a third instance -> two get 1 partition each, one sits idle
 *     (more consumers than partitions).
 *  2. Run with a DIFFERENT group.id (e.g. "aml-service") in a third terminal
 *     while the above is running -> it reads the SAME messages independently,
 *     from the beginning. This is the "multiple independent consumer groups"
 *     pattern (balance update vs AML monitoring reading the same stream).
 *  3. Manual commit + idempotent processing = at-least-once done safely:
 *     kill this process (Ctrl+C) right after "Processing..." prints but before
 *     "Committed" prints, restart it -> that message gets reprocessed
 *     (duplicate), but the idempotent check (dedupeStore) means it's a no-op
 *     the second time. That's the at-least-once + idempotency pattern from
 *     your recharge/e-voucher project.
 *
 * Run with:
 *   mvn exec:java -Dexec.mainClass="com.interviewprep.kafka.OrderConsumer" -Dexec.args="balance-service manual"
 *   mvn exec:java -Dexec.mainClass="com.interviewprep.kafka.OrderConsumer" -Dexec.args="aml-service manual"
 *
 * args[0] = consumer group id
 * args[1] = "manual" (commit after processing, safe) or "auto" (commit in background, can lose data)
 */
public class OrderConsumer {

    // simulates a DB "already processed this transaction" check -> idempotency
    private static final Set<String> dedupeStore = new HashSet<>();

    public static void main(String[] args) {
        String groupId = args.length > 0 ? args[0] : "balance-service";
        boolean manualCommit = args.length <= 1 || !"auto".equalsIgnoreCase(args[1]);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", groupId);
        props.put("auto.offset.reset", "earliest"); // read from start if no committed offset yet
        props.put("enable.auto.commit", true); // THE KEY SETTING for this demo

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TopicAdmin.TOPIC));

            System.out.printf("Consumer started. group.id=%s commit-mode=%s%n",
                    groupId, manualCommit ? "MANUAL (at-least-once, safe)" : "AUTO (can lose data)");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("[%s] partition=%d offset=%d key=%s value=%s%n",
                            groupId, record.partition(), record.offset(), record.key(), record.value());

                    processWithIdempotency(record);

                    // ---- MANUAL COMMIT: only commit AFTER processing succeeds ----
                    // this is what makes duplicate reprocessing safe instead of data-lossy
                    if (manualCommit) {
                        consumer.commitSync();
                        System.out.println("   -> Committed offset " + record.offset());
                    }
                    // if manualCommit is false, Kafka's background thread commits on its own
                    // timer (auto.commit.interval.ms, default 5s) regardless of whether the
                    // line above even ran -> that's the data-loss risk in the auto-commit case
                }
            }
        }
    }

    /** Simulates writing to a DB with a unique-constraint / dedupe check. */
    private static void processWithIdempotency(ConsumerRecord<String, String> record) {
        String dedupeKey = record.key() + ":" + record.offset();
        if (dedupeStore.contains(dedupeKey)) {
            System.out.println("   -> DUPLICATE detected, skipping DB write (idempotent no-op)");
            return;
        }
        System.out.println("   -> Processing (simulated DB write: crediting wallet)...");
        dedupeStore.add(dedupeKey);
    }
}
