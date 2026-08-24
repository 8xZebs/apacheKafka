# Kafka — Runnable IntelliJ Project

A small Maven project that lets you *see* every Kafka concept from your prep notes
actually happen, using a plain local Kafka install

## Covers
- Topics, partitions, keyed partitioning
- Consumer groups + partition assignment
- Manual vs auto offset commit (at-least-once vs risk of data loss)
- Idempotent processing (handling duplicates safely)
- `acks` durability trade-off (0 / 1 / all)
- Idempotent producer + transactions (exactly-once building blocks)
- Replication / leader-follower — explained conceptually here (single broker
  can't run RF=3, see note below)

---

## 1. Prerequisites
- Java 17+ (`java -version` to check)
- Maven (or use IntelliJ's bundled Maven)
- IntelliJ IDEA

## 2. Install Kafka natively (one-time setup)

1. Download the **binary** release (not source) from
   https://kafka.apache.org/downloads — e.g. `kafka_2.13-3.7.0.tgz`
2. Extract it to a simple path with no spaces, e.g. `C:\kafka`
   (avoid OneDrive-synced folders — long/space-y paths can break the Windows `.bat` scripts)
3. Open PowerShell, `cd C:\kafka`, and format storage for KRaft mode (one-time):
   ```powershell
   .\bin\windows\kafka-storage.bat random-uuid
   # copy the printed UUID, then:
   .\bin\windows\kafka-storage.bat format -t <paste-uuid-here> -c .\config\kraft\server.properties
   ```

## 3. Start Kafka

In that same PowerShell window:
```powershell
.\bin\windows\kafka-server-start.bat .\config\kraft\server.properties
```
**Leave this window open** — this process IS your Kafka broker. Closing it stops Kafka.
Open a **new** PowerShell window for everything else below.

## 4. Create the topic (via command line, one-time)

From `C:\kafka` in the new window:
```powershell
.\bin\windows\kafka-topics.bat --create --topic orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```
(Or just run `TopicAdmin.java` from IntelliJ instead — it does the same thing.)

## 5. Open the Java project in IntelliJ

- `File > Open` → select this `kafka-interview-demo` folder
- IntelliJ detects `pom.xml` and imports it as a Maven project automatically
- Let it download dependencies (kafka-clients, slf4j)

## 6. Exercises

### Exercise A — Partitioning & keys
Run `OrderProducer` (right-click → Run `main()`). Watch the console: the same
`subscriberId` always goes to the **same partition**; different subscriber IDs
land on different partitions.

### Exercise B — Consumer groups
Run two instances of `OrderConsumer` with args `balance-service manual` — Kafka
splits the 3 partitions between them. Add a third instance, same group → one
sits idle. Then run a fourth with a *different* group id, `aml-service manual`
— it independently reads the same messages from the start.

### Exercise C — Manual vs auto commit
Run `OrderConsumer` with `balance-service manual`. Kill it (Ctrl+C) right after
"Processing..." prints but before "Committed" prints — restart it, and that
message gets reprocessed (duplicate), caught by the idempotency check. Then try
`balance-service auto` to see the background-commit (data-loss-risk) version.

Run `OrderProducer` with args `0`, `1`, and `all` — compare behavior.
```
mvn exec:java -Dexec.mainClass="com.kafkaProject.kafka.OrderProducer" -Dexec.args="0"
mvn exec:java -Dexec.mainClass="com.kafkaProject.kafka.OrderProducer" -Dexec.args="1"
mvn exec:java -Dexec.mainClass="com.kafkaProject.kafka.OrderProducer" -Dexec.args="all"
```

### Exercise E — Exactly-once building blocks
Run `ExactlyOnceProducer`. Two messages become visible atomically inside one
transaction.

## A note on replication/leader-follower (Exercise D from the original plan)

This single-broker setup uses **replication factor 1** — there's nothing to
replicate to, so you can't physically kill a leader and watch failover happen
here. That exercise needed the 3-broker Docker setup.

That's fine for interview prep: this is genuinely the more common way people
first learn Kafka locally, and you can still describe leader/follower/ISR/
failover fluently from the explanation + diagram walkthrough we did earlier —
interviewers are testing whether you understand the *mechanism*, not whether
you've personally triggered a failover on your laptop. If you want to see it
live later, the Docker Compose version (3 brokers) is worth revisiting once
Docker Desktop is set up — but it's not blocking your prep now.

## 7. Shut down
Ctrl+C in the PowerShell window running `kafka-server-start.bat`.

---

## Quick reference: which exercise proves which interview answer

| Interview topic                          | Exercise |
|-------------------------------------------|----------|
| Why partitioning matters for scaling      | A        |
| Consumer groups + partition assignment    | B        |
| Auto-commit vs manual commit              | C        |
| At-least-once + idempotency (your project)| C        |
| `acks` durability trade-off               | D        |
| Exactly-once (idempotent producer + txn)  | E        |
| Replication / leader-follower / failover  | conceptual — see note above |
