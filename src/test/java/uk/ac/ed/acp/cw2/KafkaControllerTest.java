package uk.ac.ed.acp.cw2;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
@Disabled
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class KafkaControllerTest {

    private static final Logger log = LoggerFactory.getLogger(KafkaControllerTest.class);

    @Autowired
    private TestRestTemplate rest;

    private final String testTopic = "test_input_topic";

    @Test
    void testKafkaPutAndGet() {
        int messageCount = 5;

        // Send messages to Kafka
        ResponseEntity<String> putResponse = rest.exchange(
                "/kafka/" + testTopic + "/" + messageCount,
                org.springframework.http.HttpMethod.PUT,
                null,
                String.class
        );

        assertEquals(200, putResponse.getStatusCode().value(), "Kafka PUT should return 200 OK");
        log.info("Kafka PUT response: {}", putResponse.getBody());

        // Receive messages from Kafka
        ResponseEntity<String[]> getResponse = rest.getForEntity(
                "/kafka/" + testTopic + "/3000", String[].class);

        String[] allMessages = getResponse.getBody();
        assertNotNull(allMessages, "Kafka GET response should not be null");
        log.info("Total messages received from Kafka: {}", allMessages.length);

        // Filter only the expected messages
        List<String> expectedMessages = java.util.stream.IntStream.range(0, messageCount)
                .mapToObj(i -> String.format("{\"uid\":\"s2771099\",\"counter\":%d}", i))
                .collect(Collectors.toList());

        List<String> matched = expectedMessages.stream()
                .filter(expected -> java.util.Arrays.asList(allMessages).contains(expected))
                .collect(Collectors.toList());

        matched.forEach(m -> log.info("✔ Matched expected Kafka message: {}", m));

        assertEquals(messageCount, matched.size(),
                "Expected to find " + messageCount + " matching Kafka messages with correct uid and counter");

        // Log all retrieved messages
        for (int i = 0; i < allMessages.length; i++) {
            log.info("Kafka Message {}: {}", i, allMessages[i]);
        }
    }
}