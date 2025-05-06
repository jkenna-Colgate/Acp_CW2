package uk.ac.ed.acp.cw2;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;

import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import uk.ac.ed.acp.cw2.model.AcpMessage;
import uk.ac.ed.acp.cw2.controller.KafkaMessageReader;
import uk.ac.ed.acp.cw2.service.AcpStorageService;

import java.util.*;


import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
@Disabled
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
public class MessageControllerTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @MockBean
    private AcpStorageService acpStorageService;

    @MockBean
    private RabbitTemplate rabbitTemplate;

    @MockBean
    private KafkaMessageReader kafkaReader;

    @Test
    public void testProcessMessages_sendsCorrectMessages() throws Exception {
        // Arrange: mock Kafka input
        List<String> fakeMessages = List.of(
                "{\"uid\":\"s2771099\",\"key\":\"AAA\",\"comment\":\"ok\",\"value\":10.5}",
                "{\"uid\":\"s2771099\",\"key\":\"ABCD\",\"comment\":\"ok\",\"value\":10.5}",
                "{\"uid\":\"s2771099\",\"key\":\"ABCDE\",\"comment\":\"ok\",\"value\":10.5}"
        );
        when(kafkaReader.readMessagesFromBeginning(anyString(), eq(3))).thenReturn(fakeMessages);

        // ACP returns a fake UUID for each good message
        when(acpStorageService.storeMessage(any(AcpMessage.class)))
                .thenReturn("uuid-1")
                .thenReturn("uuid-2");

        Map<String, Object> request = new HashMap<>();
        request.put("readTopic", "test-input-topic");
        request.put("writeQueueGood", "test-good-queue");
        request.put("writeQueueBad", "test-bad-queue");
        request.put("messageCount", 3);

        // Act: POST to /processMessages
        ResponseEntity<String> response = restTemplate.postForEntity("/processMessages", request, String.class);

        // Assert: status OK
        assertEquals(HttpStatus.OK, response.getStatusCode());

        // Capture all RabbitTemplate sends
        ArgumentCaptor<String> queueCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> msgCaptor = ArgumentCaptor.forClass(String.class);
        verify(rabbitTemplate, atLeast(1)).convertAndSend(queueCaptor.capture(), msgCaptor.capture());

        List<String> queues = queueCaptor.getAllValues();
        List<String> messages = msgCaptor.getAllValues();

        // Basic sanity checks
        assertTrue(queues.contains("test-good-queue"));
        assertTrue(queues.contains("test-bad-queue"));

        // Check for TOTAL messages
        boolean goodTotalSent = messages.stream().anyMatch(msg -> msg.contains("\"key\":\"TOTAL\"") && msg.contains("\"value\":21.0"));
        boolean badTotalSent = messages.stream().anyMatch(msg -> msg.contains("\"key\":\"TOTAL\"") && msg.contains("\"value\":10.5"));

        assertTrue(goodTotalSent, "Expected TOTAL for good queue");
        assertTrue(badTotalSent, "Expected TOTAL for bad queue");

        // Check UUIDs present in good messages
        boolean uuid1Found = messages.stream().anyMatch(msg -> msg.contains("uuid-1"));
        boolean uuid2Found = messages.stream().anyMatch(msg -> msg.contains("uuid-2"));

        assertTrue(uuid1Found);
        assertTrue(uuid2Found);
    }
}
