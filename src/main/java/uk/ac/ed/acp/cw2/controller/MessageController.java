package uk.ac.ed.acp.cw2.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.HttpStatus;
import uk.ac.ed.acp.cw2.data.TransformRequest;
import uk.ac.ed.acp.cw2.model.AcpMessage;
import uk.ac.ed.acp.cw2.service.AcpStorageService;
import uk.ac.ed.acp.cw2.service.TransformService;

import java.util.*;

@RestController
public class MessageController {

    @Autowired
    private KafkaMessageReader kafkaReader;
    @Autowired
    private AcpStorageService acpStorageService;
    @Autowired
    private RabbitTemplate rabbitTemplate;
    @Autowired
    private ConnectionFactory connectionFactory;
    @Autowired
    private TransformService transformService;


    private final ObjectMapper mapper = new ObjectMapper();

    @PostMapping("/processMessages")
    public ResponseEntity<String> processMessages(@RequestBody ProcessMessagesRequest request) {
        String readTopic = request.getReadTopic();
        String writeQueueGood = request.getWriteQueueGood();
        String writeQueueBad = request.getWriteQueueBad();
        int messageCount = request.getMessageCount();

        try (Connection conn = connectionFactory.createConnection();
             Channel channel = conn.createChannel(false)) {

            channel.queueDeclare(writeQueueGood, false, false, false, null);
            channel.queueDeclare(writeQueueBad, false, false, false, null);

            System.out.println("Queues declared: " + writeQueueGood + ", " + writeQueueBad);
        } catch (Exception e) {
            System.err.println("Failed to declare queues properly");
            e.printStackTrace();
        }

        List<String> rawMessages = kafkaReader.readMessagesFromBeginning(readTopic, messageCount);

        List<AcpMessage> goodMessages = new ArrayList<>();
        List<AcpMessage> badMessages = new ArrayList<>();
        double runningTotal = classifyMessages(rawMessages, goodMessages, badMessages);

        enrichGoodMessagesWithUUID(goodMessages);

        sendMessagesToQueue(goodMessages, writeQueueGood);
        sendMessagesToQueue(badMessages, writeQueueBad);

        sendTotalMessages(writeQueueGood, writeQueueBad, goodMessages, badMessages);

        System.out.println("Processed " + messageCount + " messages from topic " + readTopic);
        System.out.println("RabbitMQ connection factory: " + rabbitTemplate.getConnectionFactory());
        return ResponseEntity.ok("Messages processed successfully.");
    }

    public double classifyMessages(List<String> rawMessages, List<AcpMessage> goodMessages, List<AcpMessage> badMessages) {
        double runningTotal = 0.0;

        for (String raw : rawMessages) {
            try {
                AcpMessage msg = mapper.readValue(raw, AcpMessage.class);
                int keyLength = msg.getKey().length();

                if (keyLength == 3 || keyLength == 4) {
                    runningTotal += msg.getValue();
                    msg.setRunningTotalValue(runningTotal);
                    goodMessages.add(msg);
                } else {
                    badMessages.add(msg);
                }
            } catch (Exception e) {
                e.printStackTrace(); // Consider proper logging
            }
        }

        return runningTotal;
    }

    private void enrichGoodMessagesWithUUID(List<AcpMessage> goodMessages) {
        for (AcpMessage msg : goodMessages) {
            String uuid = acpStorageService.storeMessage(msg);
            msg.setUuid(uuid);
        }
    }

    private void sendMessagesToQueue(List<AcpMessage> messages, String queueName) {
        for (AcpMessage msg : messages) {
            try {
                rabbitTemplate.convertAndSend(queueName, msg); // sends as JSON object
            } catch (Exception e) {
                System.err.println("Failed to send message to queue: " + queueName);
                e.printStackTrace();
            }
        }
    }

    private void sendTotalMessages(String queueGood, String queueBad, List<AcpMessage> goodMessages, List<AcpMessage> badMessages) {
        double totalGood = goodMessages.stream().mapToDouble(AcpMessage::getValue).sum();
        double totalBad = badMessages.stream().mapToDouble(AcpMessage::getValue).sum();

        String uid = !goodMessages.isEmpty()
                ? goodMessages.get(0).getUid()
                : (!badMessages.isEmpty() ? badMessages.get(0).getUid() : "sUNKNOWN");

        sendTotalMessageToQueue(queueGood, uid, totalGood);
        sendTotalMessageToQueue(queueBad, uid, totalBad);
    }

    public void sendTotalMessageToQueue(String queue, String uid, double total) {
        AcpMessage totalMsg = new AcpMessage();
        totalMsg.setUid(uid);
        totalMsg.setKey("TOTAL");
        totalMsg.setValue(total);
        totalMsg.setComment("");

        try {
            //Send object
            rabbitTemplate.convertAndSend(queue, totalMsg);
        } catch (Exception e) {
            System.err.println("Failed to send TOTAL message to queue: " + queue);
            e.printStackTrace();
        }
    }
    @PostMapping("/transformMessages")
    public ResponseEntity<String> transformMessages(@RequestBody TransformRequest request) {
        try {
            String readQueue = request.getReadQueue();
            String writeQueue = request.getWriteQueue();
            int messageCount = request.getMessageCount();

            // log or validate
            System.out.printf("Received request to transform %d messages from %s to %s%n",
                    messageCount, readQueue, writeQueue);

            // You’ll delegate real work to a service class
            transformService.handleTransformMessages(readQueue, writeQueue, messageCount);

            return ResponseEntity.ok("Transform complete");
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error processing transformMessages");
        }
    }
}
