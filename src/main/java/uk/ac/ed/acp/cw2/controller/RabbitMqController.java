package uk.ac.ed.acp.cw2.controller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.rabbit.core.RabbitTemplate;


import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * RabbitMqController is a REST controller that provides endpoints for sending and receiving stock symbols
 * through RabbitMQ. This class interacts with a RabbitMQ environment which is configured dynamically during runtime.
 */
@RestController()
public class RabbitMqController {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqController.class);
    private final RuntimeEnvironment environment;
    private final String[] stockSymbols = "AAPL,MSFT,GOOG,AMZN,TSLA,JPMC,CATP,UNIL,LLOY".split(",");

    public RabbitMqController(RuntimeEnvironment environment) {
        this.environment = environment;
    }

    /**
     * Constructs Kafka properties required for KafkaProducer and KafkaConsumer configuration.
     *
     * @param environment the runtime environment providing dynamic configuration details
     *                     such as Kafka bootstrap servers.
     * @return a Properties object containing configuration properties for Kafka operations.
     */

    public final String StockSymbolsConfig = "stock.symbols";

    @PutMapping("/rabbitMq/{queueName}/{messageCount}")
    public ResponseEntity<String> putMessagesToRabbitMq(@PathVariable String queueName,
                                                        @PathVariable int messageCount) {
        logger.info("Sending {} messages to RabbitMQ queue '{}'", messageCount, queueName);

        RabbitTemplate rabbitTemplate = environment.getRabbitTemplate();
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            rabbitTemplate.execute(channel -> {
                channel.queueDeclare(queueName, false, false, false, null);
                return null;
            });

            for (int i = 0; i < messageCount; i++) {
                Map<String, Object> message = new HashMap<>();
                message.put("uid", "s2771099");
                message.put("counter", i);

                String json = objectMapper.writeValueAsString(message);
                rabbitTemplate.convertAndSend("", queueName, json);
            }

        } catch (Exception e) {
            logger.error("Failed to send messages to RabbitMQ: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body("Failed to send messages to RabbitMQ: " + e.getMessage());
        }

        return ResponseEntity.ok("Successfully sent " + messageCount + " messages to " + queueName);
    }
    @GetMapping("/rabbitMq/{queueName}/{timeoutInMsec}")
    public ResponseEntity<List<String>> getMessagesFromRabbitMq(
            @PathVariable String queueName,
            @PathVariable int timeoutInMsec) {

        logger.info("Reading messages from queue '{}' with timeout {} ms", queueName, timeoutInMsec);

        RabbitTemplate rabbitTemplate = environment.getRabbitTemplate();
        List<String> messages = new ArrayList<>();

        long startTime = System.currentTimeMillis();
        long deadline = startTime + timeoutInMsec;

        while (System.currentTimeMillis() < deadline) {
            Object message = rabbitTemplate.receiveAndConvert(queueName);
            if (message != null) {
                messages.add(message.toString());
            } else {
                try {
                    Thread.sleep(10);  // get rid of try catch b/c unnecessary
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Polling interrupted", e);
                    break;
                }
            }
        }

        logger.info("Returning {} messages from queue '{}'", messages.size(), queueName);
        return ResponseEntity.ok(messages);
    }



}
