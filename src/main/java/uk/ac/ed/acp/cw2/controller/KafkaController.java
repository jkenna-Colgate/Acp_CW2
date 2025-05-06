package uk.ac.ed.acp.cw2.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StopWatch;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * KafkaController is a REST API controller used to interact with Apache Kafka for producing
 * and consuming stock symbol events. This class provides endpoints for sending stock symbols
 * to a Kafka topic and retrieving stock symbols from a Kafka topic.
 * <p>
 * It is designed to handle dynamic Kafka configurations based on the runtime environment
 * and supports security configurations such as SASL and JAAS.
 */
@RestController()
public class KafkaController {

    private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);
    private final RuntimeEnvironment environment;
    private final String[] stockSymbols = "AAPL,MSFT,GOOG,AMZN,TSLA,JPMC,CATP,UNIL,LLOY".split(",");

    public KafkaController(RuntimeEnvironment environment) {
        this.environment = environment;
    }

    /**
     * Constructs Kafka properties required for KafkaProducer and KafkaConsumer configuration.
     *
     * @param environment the runtime environment providing dynamic configuration details
     *                     such as Kafka bootstrap servers.
     * @return a Properties object containing configuration properties for Kafka operations.
     */
    private Properties getKafkaProperties(RuntimeEnvironment environment) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", environment.getKafkaBootstrapServers());
        kafkaProps.put("acks", "all");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.setProperty("enable.auto.commit", "true");
        kafkaProps.put("acks", "all");

        kafkaProps.put("group.id", UUID.randomUUID().toString());
        kafkaProps.setProperty("auto.offset.reset", "earliest");
        kafkaProps.setProperty("enable.auto.commit", "true");

        if (environment.getKafkaSecurityProtocol() != null) {
            kafkaProps.put("security.protocol", environment.getKafkaSecurityProtocol());
        }
        if (environment.getKafkaSaslMechanism() != null) {
            kafkaProps.put("sasl.mechanism", environment.getKafkaSaslMechanism());
        }
        if (environment.getKafkaSaslJaasConfig() != null) {
            kafkaProps.put("sasl.jaas.config", environment.getKafkaSaslJaasConfig());
        }

        return kafkaProps;
    }
    @PutMapping("/kafka/{writeTopic}/{messageCount}")
    public ResponseEntity<String> writeToKafka(@PathVariable String writeTopic,
                                               @PathVariable int messageCount) {
        System.out.println("Kafka write invoked with topic=" + writeTopic + ", count=" + messageCount);
        Properties kafkaProps = getKafkaProperties(environment);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps)) {
            ObjectMapper mapper = new ObjectMapper();

            for (int i = 0; i < messageCount; i++) {
                Map<String, Object> message = new HashMap<>();
                message.put("uid", "s2771099");
                message.put("counter", i);
                String jsonMessage = mapper.writeValueAsString(message);
                logger.info("Sending: {}", jsonMessage);
                producer.send(new ProducerRecord<>(writeTopic, null, jsonMessage));
            }
            producer.flush();
            return ResponseEntity.ok("Successfully wrote " + messageCount + " messages to Kafka topic " + writeTopic);
        } catch (Exception e) {
            logger.error("Error while writing to Kafka topic {}: {}", writeTopic, e.getMessage(), e);
            e.printStackTrace();
            return ResponseEntity.status(500).body("Kafka write failed: " + e.getMessage());
        }
    }
    @GetMapping("/kafka/{readTopic}/{timeoutInMsec}")
    public ResponseEntity<List<String>> getMessagesFromKafka(
            @PathVariable String readTopic,
            @PathVariable int timeoutInMsec) {

        logger.info("Reading from Kafka topic '{}' for {} ms", readTopic, timeoutInMsec);
        Properties kafkaProps = getKafkaProperties(environment);
        List<String> messages = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps)) {
            // 1) Discover all partitions for the topic
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(readTopic);
            List<TopicPartition> partitions = partitionInfos.stream()
                    .map(pi -> new TopicPartition(readTopic, pi.partition()))
                    .collect(Collectors.toList());

            // 2) Manually assign and seek-to-beginning instantly
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            // 3) Time‑boxed polling with early exit once drained
            long deadline = System.currentTimeMillis() + timeoutInMsec;
            int emptyCount = 0, maxEmpty = 2;
            while (System.currentTimeMillis() < deadline && emptyCount < maxEmpty) {
                long timeLeft = deadline - System.currentTimeMillis();
                long pollMs   = Math.min(50, timeLeft);
                ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(pollMs));

                if (recs.isEmpty()) {
                    emptyCount++;
                } else {
                    emptyCount = 0;
                    for (var r : recs) {
                        messages.add(r.value());
                    }
                }
            }

            long elapsed = System.currentTimeMillis() - (deadline - timeoutInMsec);
            logger.info("Retrieved {} messages from '{}' in {} ms", messages.size(), readTopic, elapsed);
            return ResponseEntity.ok(messages);

        } catch (Exception e) {
            logger.error("Failed to read from Kafka topic", e);
            return ResponseEntity.status(500).body(Collections.emptyList());
        }
    }
}
