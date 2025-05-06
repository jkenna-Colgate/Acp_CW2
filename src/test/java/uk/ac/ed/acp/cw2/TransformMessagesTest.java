package uk.ac.ed.acp.cw2;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import uk.ac.ed.acp.cw2.service.RedisService;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
@Disabled
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TransformMessagesTest {

    private static final Logger log = LoggerFactory.getLogger(TransformMessagesTest.class);

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private RedisService redisService;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final String readQueue = "test_read_queue";
    private final String writeQueue = "test_write_queue";

    @BeforeEach
    void clearRedisAndQueues() throws Exception {
        redisService.clearAll();

        rabbitTemplate.execute(channel -> {
            channel.queueDeclare(readQueue, false, false, false, null);
            channel.queueDeclare(writeQueue, false, false, false, null);
            return null;
        });
        // Purge any existing messages
        for (int i = 0; i < 10; i++) {
            rabbitTemplate.receiveAndConvert(readQueue);
            rabbitTemplate.receiveAndConvert(writeQueue);
        }

    }

    @Test
    void testLevel1_ABC_V1_TransformsTo_110_5() throws Exception {

        // Create the message and send it directly to the read queue
        Map<String, Object> msg = new HashMap<>();
        msg.put("key", "ABC");
        msg.put("version", 1);
        msg.put("value", 100.0);

        rabbitTemplate.convertAndSend(readQueue, msg);

        // Prepare and send POST /transformMessages
        Map<String, Object> transformRequest = Map.of(
                "readQueue", readQueue,
                "writeQueue", writeQueue,
                "messageCount", 1
        );
        ResponseEntity<String> postResp = rest.postForEntity("/transformMessages", transformRequest, String.class);
        assertEquals(HttpStatus.OK, postResp.getStatusCode(), "POST transformMessages should return 200 OK");

        // Read from write queue and validate
        Object message = rabbitTemplate.receiveAndConvert(writeQueue, 3000);
        assertNotNull(message, "Expected one message in output queue");

        @SuppressWarnings("unchecked")
        Map<String, Object> output = (Map<String, Object>) message;

        log.info("Transform Level 1 Output: {}", output);
        assertEquals("ABC", output.get("key"));
        assertEquals(1, output.get("version"));
        assertEquals(110.5, ((Number) output.get("value")).doubleValue());
    }
    @Test
    void testLevel2_RepeatABC_V1_PassesThroughUnchanged() throws Exception {
        // First message to initialize Redis with ABC v1
        Map<String, Object> initialMsg = Map.of(
                "key", "ABC",
                "version", 1,
                "value", 100.0
        );
        rabbitTemplate.convertAndSend(readQueue, initialMsg);
        rest.postForEntity("/transformMessages", Map.of(
                "readQueue", readQueue,
                "writeQueue", writeQueue,
                "messageCount", 1
        ), String.class);

        // Clear write queue for clean check
        rabbitTemplate.receiveAndConvert(writeQueue); // discard old msg

        // Send the same version again
        Map<String, Object> repeatMsg = Map.of(
                "key", "ABC",
                "version", 1,
                "value", 100.0
        );
        rabbitTemplate.convertAndSend(readQueue, repeatMsg);

        ResponseEntity<String> resp = rest.postForEntity("/transformMessages", Map.of(
                "readQueue", readQueue,
                "writeQueue", writeQueue,
                "messageCount", 1
        ), String.class);
        assertEquals(HttpStatus.OK, resp.getStatusCode());

        Object message = rabbitTemplate.receiveAndConvert(writeQueue, 3000);
        assertNotNull(message, "Expected one message in output queue");
        @SuppressWarnings("unchecked")
        Map<String, Object> output = (Map<String, Object>) message;

        log.info("Transform Level 2 Output: {}", output);
        assertEquals("ABC", output.get("key"));
        assertEquals(1, output.get("version"));
        assertEquals(100.0, ((Number) output.get("value")).doubleValue());
    }
    @Test
    void testLevel3_ABC_V3_BumpsValueAndUpdatesRedis() throws Exception {
        // First message to initialize Redis with ABC v1
        Map<String, Object> v1Msg = Map.of(
                "key", "ABC",
                "version", 1,
                "value", 100.0
        );
        rabbitTemplate.convertAndSend(readQueue, v1Msg);
        rest.postForEntity("/transformMessages", Map.of(
                "readQueue", readQueue,
                "writeQueue", writeQueue,
                "messageCount", 1
        ), String.class);
        rabbitTemplate.receiveAndConvert(writeQueue); // Clear output

        // Now send a newer version
        Map<String, Object> v3Msg = Map.of(
                "key", "ABC",
                "version", 3,
                "value", 400.0
        );
        rabbitTemplate.convertAndSend(readQueue, v3Msg);

        ResponseEntity<String> resp = rest.postForEntity("/transformMessages", Map.of(
                "readQueue", readQueue,
                "writeQueue", writeQueue,
                "messageCount", 1
        ), String.class);
        assertEquals(HttpStatus.OK, resp.getStatusCode());

        Object message = rabbitTemplate.receiveAndConvert(writeQueue, 3000);
        assertNotNull(message, "Expected one message in output queue");
        @SuppressWarnings("unchecked")
        Map<String, Object> output = (Map<String, Object>) message;

        log.info("Transform Level 3 Output: {}", output);
        assertEquals("ABC", output.get("key"));
        assertEquals(3, output.get("version"));
        assertEquals(410.5, ((Number) output.get("value")).doubleValue(), 0.01);
    }
    @Test
    void testLevel4_ABC_V1_PassedThroughIfOlder() throws Exception {
        // Step 1: Put ABC v3 into Redis
        Map<String, Object> v3Msg = Map.of(
                "key", "ABC",
                "version", 3,
                "value", 400.0
        );
        rabbitTemplate.convertAndSend(readQueue, v3Msg);
        rest.postForEntity("/transformMessages", Map.of(
                "readQueue", readQueue,
                "writeQueue", writeQueue,
                "messageCount", 1
        ), String.class);
        rabbitTemplate.receiveAndConvert(writeQueue); // clear

        // Step 2: Send older v1 message
        Map<String, Object> v1Msg = Map.of(
                "key", "ABC",
                "version", 1,
                "value", 100.0
        );
        rabbitTemplate.convertAndSend(readQueue, v1Msg);
        ResponseEntity<String> resp = rest.postForEntity("/transformMessages", Map.of(
                "readQueue", readQueue,
                "writeQueue", writeQueue,
                "messageCount", 1
        ), String.class);
        assertEquals(HttpStatus.OK, resp.getStatusCode());

        Object message = rabbitTemplate.receiveAndConvert(writeQueue, 3000);
        assertNotNull(message, "Expected one message in output queue");
        @SuppressWarnings("unchecked")
        Map<String, Object> output = (Map<String, Object>) message;

        log.info("Transform Level 4 Output: {}", output);
        assertEquals("ABC", output.get("key"));
        assertEquals(1, output.get("version"));
        assertEquals(100.0, ((Number) output.get("value")).doubleValue(), 0.01);
    }
    @Test
    void testLevel5_TombstoneClearsRedisAndSendsSummary() throws Exception {
        // Step 1: Pre-load Redis with ABC v1
        rabbitTemplate.convertAndSend(readQueue, Map.of(
                "key", "ABC",
                "version", 1,
                "value", 100.0
        ));
        rest.postForEntity("/transformMessages", Map.of(
                "readQueue", readQueue,
                "writeQueue", writeQueue,
                "messageCount", 1
        ), String.class);
        rabbitTemplate.receiveAndConvert(writeQueue); // clear

        // Step 2: Send tombstone message
        rabbitTemplate.convertAndSend(readQueue, Map.of("key", "ABC"));
        ResponseEntity<String> resp = rest.postForEntity("/transformMessages", Map.of(
                "readQueue", readQueue,
                "writeQueue", writeQueue,
                "messageCount", 1
        ), String.class);
        assertEquals(HttpStatus.OK, resp.getStatusCode());

        Object output = rabbitTemplate.receiveAndConvert(writeQueue, 3000);
        assertNotNull(output, "Expected a summary message in output queue");

        @SuppressWarnings("unchecked")
        Map<String, Object> summary = (Map<String, Object>) output;
        log.info("Transform Level 5 Output (Summary): {}", summary);

        assertTrue(summary.containsKey("totalMessagesProcessed"));
        assertTrue(summary.containsKey("totalMessagesWritten"));
        assertTrue(summary.containsKey("totalRedisUpdates"));
        assertTrue(summary.containsKey("totalValueWritten"));
        assertTrue(summary.containsKey("totalAdded"));
    }
    @Test
    void testLevel6_TombstoneFollowedByNewVersion() throws Exception {
        // Tombstone message to clear redis
        Map<String, Object> tombstone = Map.of("key", "ABC");
        rabbitTemplate.convertAndSend(readQueue, tombstone);

        // New message after tombstone
        Map<String, Object> msg = new HashMap<>();
        msg.put("key", "ABC");
        msg.put("version", 2);
        msg.put("value", 200.0);
        rabbitTemplate.convertAndSend(readQueue, msg);

        Map<String, Object> transformRequest = Map.of(
                "readQueue", readQueue,
                "writeQueue", writeQueue,
                "messageCount", 2
        );

        ResponseEntity<String> postResp = rest.postForEntity("/transformMessages", transformRequest, String.class);
        assertEquals(HttpStatus.OK, postResp.getStatusCode(), "POST transformMessages should return 200 OK");

        // First message: tombstone summary
        Object summaryMsg = rabbitTemplate.receiveAndConvert(writeQueue, 3000);
        assertNotNull(summaryMsg);
        Map<String, Object> summary = (Map<String, Object>) summaryMsg;
        log.info("Transform Level 6 Summary: {}", summary);
        assertEquals(1, summary.get("totalMessagesProcessed"));
        assertEquals(0, summary.get("totalMessagesWritten"));

        // Second message: transformed ABC
        Object newMsg = rabbitTemplate.receiveAndConvert(writeQueue, 3000);
        assertNotNull(newMsg);
        Map<String, Object> transformed = (Map<String, Object>) newMsg;
        log.info("Transform Level 6 Output: {}", transformed);
        assertEquals("ABC", transformed.get("key"));
        assertEquals(2, transformed.get("version"));
        assertEquals(210.5, ((Number) transformed.get("value")).doubleValue());
    }
    @Test
    void testLevel7_MixedVersionsWritesCorrectly() throws Exception {
        rabbitTemplate.convertAndSend(readQueue, Map.of("key", "ABC", "version", 1, "value", 100.0));
        rabbitTemplate.convertAndSend(readQueue, Map.of("key", "ABC", "version", 3, "value", 400.0));
        rabbitTemplate.convertAndSend(readQueue, Map.of("key", "ABC", "version", 2, "value", 200.0));

        Map<String, Object> transformRequest = Map.of(
                "readQueue", readQueue,
                "writeQueue", writeQueue,
                "messageCount", 3
        );

        ResponseEntity<String> postResp = rest.postForEntity("/transformMessages", transformRequest, String.class);
        assertEquals(HttpStatus.OK, postResp.getStatusCode());

        Set<Double> expectedValues = Set.of(110.5, 410.5, 200.0);
        Set<Double> actualValues = new HashSet<>();

        for (int i = 0; i < 3; i++) {
            Object raw = rabbitTemplate.receiveAndConvert(writeQueue, 3000);
            assertNotNull(raw, "Expected message " + i);
            @SuppressWarnings("unchecked")
            Map<String, Object> out = (Map<String, Object>) raw;
            log.info("Transform Level 7 Output: {}", out);
            actualValues.add(((Number) out.get("value")).doubleValue());
        }

        assertEquals(expectedValues, actualValues, "Output values should match expected values exactly");
    }
    @Test
    void testLevel8_MixedThenTombstone() throws Exception {
        // Preload normal messages
        rabbitTemplate.convertAndSend(readQueue, Map.of("key", "ABC", "version", 1, "value", 100.0));
        rabbitTemplate.convertAndSend(readQueue, Map.of("key", "ABC", "version", 3, "value", 400.0));

        // Follow with tombstone
        rabbitTemplate.convertAndSend(readQueue, Map.of("key", "ABC"));

        Map<String, Object> transformRequest = Map.of(
                "readQueue", readQueue,
                "writeQueue", writeQueue,
                "messageCount", 3
        );

        ResponseEntity<String> postResp = rest.postForEntity("/transformMessages", transformRequest, String.class);
        assertEquals(HttpStatus.OK, postResp.getStatusCode());

        List<Map<String, Object>> results = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Object msg = rabbitTemplate.receiveAndConvert(writeQueue, 3000);
            assertNotNull(msg, "Expected message " + i);
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) msg;
            results.add(map);
            log.info("Transform Level 8 Output: {}", map);
        }

        boolean found110_5 = results.stream().anyMatch(m -> m.get("version").equals(1) && ((Number) m.get("value")).doubleValue() == 110.5);
        boolean found410_5 = results.stream().anyMatch(m -> m.get("version").equals(3) && ((Number) m.get("value")).doubleValue() == 410.5);
        boolean foundSummary = results.stream().anyMatch(m -> m.containsKey("totalMessagesProcessed"));

        assertTrue(found110_5, "Should contain v1 with value 110.5");
        assertTrue(found410_5, "Should contain v3 with value 410.5");
        assertTrue(foundSummary, "Should contain tombstone summary");
    }
    @Test
    void testLevel9_TombstoneThenNewMessage() throws Exception {
        // First v1
        rabbitTemplate.convertAndSend(readQueue, Map.of("key", "ABC", "version", 1, "value", 100.0));
        // Tombstone
        rabbitTemplate.convertAndSend(readQueue, Map.of("key", "ABC"));
        // Same v1 again — should behave like new
        rabbitTemplate.convertAndSend(readQueue, Map.of("key", "ABC", "version", 1, "value", 100.0));

        Map<String, Object> transformRequest = Map.of(
                "readQueue", readQueue,
                "writeQueue", writeQueue,
                "messageCount", 3
        );

        ResponseEntity<String> postResp = rest.postForEntity("/transformMessages", transformRequest, String.class);
        assertEquals(HttpStatus.OK, postResp.getStatusCode());

        List<Map<String, Object>> results = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Object msg = rabbitTemplate.receiveAndConvert(writeQueue, 3000);
            assertNotNull(msg, "Expected message " + i);
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) msg;
            results.add(map);
            log.info("Transform Level 9 Output: {}", map);
        }

        // Check for TWO v1 bumped messages
        long bumpedV1Count = results.stream().filter(m ->
                "ABC".equals(m.get("key")) &&
                        m.get("version").equals(1) &&
                        ((Number) m.get("value")).doubleValue() == 110.5
        ).count();
        assertEquals(2, bumpedV1Count, "Expected 2 bumped v1 messages");

        // Check for tombstone summary
        boolean foundSummary = results.stream().anyMatch(m -> m.containsKey("totalMessagesProcessed"));
        assertTrue(foundSummary, "Expected tombstone summary message");
    }

}