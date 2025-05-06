package uk.ac.ed.acp.cw2;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import uk.ac.ed.acp.cw2.controller.ProcessMessagesRequest;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
@Disabled
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ProcessMessagesTest {

    private static final Logger log = LoggerFactory.getLogger(ProcessMessagesTest.class);

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private RuntimeEnvironment environment;

    private final ObjectMapper mapper = new ObjectMapper();


    private final String queueGood = "test_good_queue";
    private final String queueBad = "test_bad_queue";

    @BeforeEach
    void clearRabbitQueues() {
        rabbitTemplate.execute(channel -> {
            channel.queueDeclare(queueGood, false, false, false, null);
            channel.queueDeclare(queueBad, false, false, false, null);
            return null;
        });

        for (int i = 0; i < 10; i++) {
            rabbitTemplate.receiveAndConvert(queueGood);
            rabbitTemplate.receiveAndConvert(queueBad);
        }
    }
    private void sendToKafka(String topic, String jsonPayload) {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(topic, jsonPayload));
            producer.flush();
        }
    }

    @Test
    void testLevel1_withRealKafka_oneValidMessageResultsInGoodAndTotal() throws Exception {
        String topic = "test_topic_" + UUID.randomUUID();
        String validAcpMessage = "{\"uid\":\"s2771099\",\"key\":\"ABC\",\"value\":123.45}";
        sendToKafka(topic, validAcpMessage);

        // Submit POST to processMessages
        ProcessMessagesRequest request = new ProcessMessagesRequest();
        request.setReadTopic(topic);
        request.setWriteQueueGood(queueGood);
        request.setWriteQueueBad(queueBad);
        request.setMessageCount(1);

        ResponseEntity<String> response = rest.postForEntity("/processMessages", request, String.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());

        // Expect 2 messages in good queue: one valid + one TOTAL
        List<Map<String, Object>> results = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Object obj = rabbitTemplate.receiveAndConvert(queueGood, 3000);
            assertNotNull(obj);
            Map<String, Object> map = mapper.readValue(obj.toString(), Map.class);
            log.info("Received from good queue: {}", map);
            results.add(map);
        }

        boolean hasABC = results.stream().anyMatch(msg ->
                "ABC".equals(msg.get("key")) &&
                        ((Number) msg.get("value")).doubleValue() == 123.45);
        boolean hasTotal = results.stream().anyMatch(msg ->
                "TOTAL".equals(msg.get("key")) &&
                        ((Number) msg.get("value")).doubleValue() == 123.45);

        assertTrue(hasABC, "Expected message with key ABC and value 123.45");
        assertTrue(hasTotal, "Expected TOTAL message with value 123.45");

        Object badTotal = rabbitTemplate.receiveAndConvert(queueBad, 1000);
        assertNotNull(badTotal, "Expected a TOTAL message in bad queue");

        Map<String, Object> badMap = mapper.readValue(badTotal.toString(), Map.class);
        log.info("Received from bad queue: {}", badMap);

        assertEquals("TOTAL", badMap.get("key"));
        assertEquals(0.0, ((Number) badMap.get("value")).doubleValue());

    }
    @Test
    void testLevel2_withRealKafka_oneInvalidMessageGoesToBadQueue() throws Exception {
        String topic = "test_topic_" + UUID.randomUUID();
        String queueGood = "test_good_queue";
        String queueBad = "test_bad_queue";


        // Send invalid message (e.g., key too short) to Kafka
        String invalidMessage = "{\"uid\":\"s2771099\",\"key\":\"XY\",\"value\":123.45}";
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(topic, invalidMessage));
            producer.flush();
        }

        // Trigger processMessages
        ProcessMessagesRequest request = new ProcessMessagesRequest();
        request.setReadTopic(topic);
        request.setWriteQueueGood(queueGood);
        request.setWriteQueueBad(queueBad);
        request.setMessageCount(1);

        ResponseEntity<String> response = rest.postForEntity("/processMessages", request, String.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());

        // Expect TOTAL in bad queue, and original invalid message
        List<Map<String, Object>> badResults = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Object obj = rabbitTemplate.receiveAndConvert(queueBad, 3000);
            assertNotNull(obj);
            Map<String, Object> map = mapper.readValue(obj.toString(), Map.class);
            log.info("Level 2 QueueBad Output: {}", map);
            badResults.add(map);
        }

        boolean hasXY = badResults.stream().anyMatch(msg ->
                "XY".equals(msg.get("key")) &&
                        ((Number) msg.get("value")).doubleValue() == 123.45);
        boolean hasTotal = badResults.stream().anyMatch(msg ->
                "TOTAL".equals(msg.get("key")) &&
                        ((Number) msg.get("value")).doubleValue() == 123.45);

        assertTrue(hasXY, "Expected invalid message with key XY in bad queue");
        assertTrue(hasTotal, "Expected TOTAL message in bad queue with value 123.45");

        Object good = rabbitTemplate.receiveAndConvert(queueGood, 3000);
        assertNotNull(good, "Expected TOTAL message in good queue even if no valid messages");

        Map<String, Object> goodTotal = mapper.readValue(good.toString(), Map.class);
        assertEquals("TOTAL", goodTotal.get("key"));
        assertEquals(0.0, ((Number) goodTotal.get("value")).doubleValue(), "Expected TOTAL message value to be 0.0");
    }
    @Test
    void testLevel3_withRealKafka_mixedMessagesHandledCorrectly() throws Exception {
        String topic = "test_topic_" + UUID.randomUUID();

        // Send 1 valid and 1 invalid message to Kafka
        sendToKafka(topic, mapper.writeValueAsString(Map.of(
                "uid", "s2771099",
                "key", "ABCD",  // Valid: 4-character key
                "value", 100.0
        )));
        sendToKafka(topic, mapper.writeValueAsString(Map.of(
                "uid", "s2771099",
                "key", "X",  // Invalid: 1-character key
                "value", 111.0
        )));

        // Submit POST to processMessages
        ProcessMessagesRequest request = new ProcessMessagesRequest();
        request.setReadTopic(topic);
        request.setWriteQueueGood(queueGood);
        request.setWriteQueueBad(queueBad);
        request.setMessageCount(2);

        ResponseEntity<String> response = rest.postForEntity("/processMessages", request, String.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());

        // GOOD queue should have 2 messages (valid + TOTAL)
        List<Map<String, Object>> goodMsgs = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Object msg = rabbitTemplate.receiveAndConvert(queueGood, 3000);
            assertNotNull(msg, "Expected a message in good queue");
            Map<String, Object> map = mapper.readValue(msg.toString(), Map.class);
            log.info("Level 3 QueueGood Output: {}", map);
            goodMsgs.add(map);
        }

        boolean hasValid = goodMsgs.stream().anyMatch(m ->
                "ABCD".equals(m.get("key")) && ((Number) m.get("value")).doubleValue() == 100.0);
        boolean hasGoodTotal = goodMsgs.stream().anyMatch(m ->
                "TOTAL".equals(m.get("key")) && ((Number) m.get("value")).doubleValue() == 100.0);
        assertTrue(hasValid);
        assertTrue(hasGoodTotal);

        // BAD queue should have 2 messages (invalid + TOTAL)
        List<Map<String, Object>> badMsgs = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Object msg = rabbitTemplate.receiveAndConvert(queueBad, 3000);
            assertNotNull(msg, "Expected a message in bad queue");
            Map<String, Object> map = mapper.readValue(msg.toString(), Map.class);
            log.info("Level 3 QueueBad Output: {}", map);
            badMsgs.add(map);
        }

        boolean hasInvalid = badMsgs.stream().anyMatch(m ->
                "X".equals(m.get("key")) && ((Number) m.get("value")).doubleValue() == 111.0);
        boolean hasBadTotal = badMsgs.stream().anyMatch(m ->
                "TOTAL".equals(m.get("key")) && ((Number) m.get("value")).doubleValue() == 111.0);
        assertTrue(hasInvalid);
        assertTrue(hasBadTotal);
    }
    @Test
    void testLevel4_withRealKafka_realAcpStorage_fullPipeline() throws Exception {
        String topic = "test_topic_" + UUID.randomUUID();
        String queueGood = "test_good_queue";
        String queueBad = "test_bad_queue";


        String inputJson = "{\"uid\":\"s2771099\",\"key\":\"XYZ\",\"value\":42.0}";
        sendToKafka(topic, inputJson);

        ProcessMessagesRequest request = new ProcessMessagesRequest();
        request.setReadTopic(topic);
        request.setWriteQueueGood(queueGood);
        request.setWriteQueueBad(queueBad);
        request.setMessageCount(1);

        ResponseEntity<String> response = rest.postForEntity("/processMessages", request, String.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());

        // Expect two messages in the good queue
        List<Map<String, Object>> goodMessages = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Object raw = rabbitTemplate.receiveAndConvert(queueGood, 3000);
            assertNotNull(raw, "Expected message in good queue");
            Map<String, Object> map = mapper.readValue(raw.toString(), Map.class);
            log.info("Level 4 Good Queue: {}", map);
            goodMessages.add(map);
        }

        boolean hasXYZ = goodMessages.stream().anyMatch(msg ->
                "XYZ".equals(msg.get("key")) &&
                        ((Number) msg.get("value")).doubleValue() == 42.0 &&
                        msg.get("uuid") != null
        );

        boolean hasTotal = goodMessages.stream().anyMatch(msg ->
                "TOTAL".equals(msg.get("key")) &&
                        ((Number) msg.get("value")).doubleValue() == 42.0
        );

        assertTrue(hasXYZ, "Expected enriched message with key XYZ");
        assertTrue(hasTotal, "Expected TOTAL message");

        Object badTotal = rabbitTemplate.receiveAndConvert(queueBad, 1000);
        assertNotNull(badTotal, "Expected a TOTAL message in bad queue");

        Map<String, Object> badMap = mapper.readValue(badTotal.toString(), Map.class);
        log.info("Received from bad queue: {}", badMap);

        assertEquals("TOTAL", badMap.get("key"));
        assertEquals(0.0, ((Number) badMap.get("value")).doubleValue());
    }
}
