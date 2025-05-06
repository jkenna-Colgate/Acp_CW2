package uk.ac.ed.acp.cw2.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.rabbit.connection.Connection;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ed.acp.cw2.model.SummaryMessage;
import uk.ac.ed.acp.cw2.model.TransformedMessage;

@Service
public class TransformService {

    private static final Logger logger = LoggerFactory.getLogger(TransformService.class);
    private final RabbitTemplate rabbitTemplate;
    private final RedisService redisService;

    public TransformService(RabbitTemplate rabbitTemplate, RedisService redisService) {
        this.rabbitTemplate = rabbitTemplate;
        this.redisService = redisService;
    }

    public void handleTransformMessages(String readQueue, String writeQueue, int messageCount) {
        logger.info("Starting transform: reading {} messages from '{}' to '{}'", messageCount, readQueue, writeQueue);
        try (Connection springConn = rabbitTemplate.getConnectionFactory().createConnection()) {
            Channel channel = springConn.createChannel(false); //

            channel.queueDeclare(writeQueue, false, false, false, null); //
        } catch (Exception e) {
            logger.error("Failed to declare write queue '{}': {}", writeQueue, e.getMessage(), e);
            throw new RuntimeException("Queue declaration failed", e);
        }
        int totalMessagesWritten = 0;
        int totalMessagesProcessed = 0;
        int totalRedisUpdates = 0;
        double totalValueWritten = 0.0;
        double totalAdded = 0.0;

        for (int i = 0; i < messageCount; i++) {
            Object msgObj = rabbitTemplate.receiveAndConvert(readQueue);
            if (msgObj == null) continue;

            totalMessagesProcessed++;

            try {
                Map<String, Object> message = new ObjectMapper().convertValue(msgObj, new TypeReference<>() {});
                logger.info("Processing message: {}", message);
                if (message.containsKey("value")) {
                    // NORMAL message
                    String key = (String) message.get("key");
                    int version = (int) message.get("version");
                    double value = ((Number) message.get("value")).doubleValue();

                    Integer existingVersion = redisService.getVersion(key);
                    if (existingVersion == null || existingVersion < version) {
                        logger.info("Updating Redis: setting key='{}' to version={}", key, version);
                    } else {
                        logger.info("Skipping Redis update for key='{}' (existing version {} >= {})", key, existingVersion, version);
                    }
                    double outputValue;
                    if (existingVersion == null || existingVersion < version) {
                        redisService.setVersion(key, version);
                        outputValue = value + 10.5;
                        totalRedisUpdates++;
                        totalAdded += 10.5;
                    } else {
                        outputValue = value;
                    }

                    TransformedMessage outputMsg = new TransformedMessage(key, version, outputValue);
                    rabbitTemplate.convertAndSend(writeQueue, outputMsg);

                    totalMessagesWritten++;
                    totalValueWritten += outputValue;

                } else {
                    // TOMBSTONE message
                    SummaryMessage tombstoneSummary = new SummaryMessage(
                            totalMessagesWritten,
                            totalMessagesProcessed,
                            totalRedisUpdates,
                            totalValueWritten,
                            totalAdded
                    );
                    rabbitTemplate.convertAndSend(writeQueue, tombstoneSummary);

                }

            } catch (Exception e) {
                logger.error("Error processing message #{}: {}", i, e.getMessage(), e);

            }
        }
    }
}
