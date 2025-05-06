package uk.ac.ed.acp.cw2;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.ResponseEntity;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
@Disabled
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RabbitMqControllerTest {

    @Autowired
    private TestRestTemplate rest;

    private final String testQueue = "test_read_queue";

    @Test
    void testRabbitMqPutAndGet() {
        int messageCount = 5;

        // Send messages to RabbitMQ
        rest.put("/rabbitMq/" + testQueue + "/" + messageCount, null);

        // Fetch messages back from RabbitMQ
        ResponseEntity<String[]> getResponse = rest
                .getForEntity("/rabbitMq/" + testQueue + "/2000", String[].class);

        String[] messages = getResponse.getBody();
        assertNotNull(messages, "GET response should not be null");
        assertEquals(messageCount, messages.length, "Should return exactly " + messageCount + " messages");

        for (int i = 0; i < messages.length; i++) {
            System.out.println("RabbitMQ Message " + i + ": " + messages[i]);
        }
    }

}