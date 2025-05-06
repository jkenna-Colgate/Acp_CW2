package uk.ac.ed.acp.cw2;

import org.junit.jupiter.api.Test;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;
import uk.ac.ed.acp.cw2.model.AcpMessage;
import uk.ac.ed.acp.cw2.service.AcpStorageService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class AcpStorageServiceTest {

    @Test
    public void testStoreMessage_successfulCall_returnsUuid() {
        // Arrange
        AcpMessage msg = new AcpMessage();
        msg.setUid("s2771099");
        msg.setKey("ABC");
        msg.setComment("Test");
        msg.setValue(9.9);
        msg.setRunningTotalValue(9.9);

        RestTemplate mockRestTemplate = mock(RestTemplate.class);
        AcpStorageService storageService = new AcpStorageService(mockRestTemplate);

        String fakeUuid = "mock-uuid-123";
        ResponseEntity<String> mockResponse = new ResponseEntity<>(fakeUuid, HttpStatus.OK);

        when(mockRestTemplate.postForEntity(
                contains("/api/v1/blob"),
                any(HttpEntity.class),
                eq(String.class))
        ).thenReturn(mockResponse);

        // Act
        String result = storageService.storeMessage(msg);

        // Assert
        assertEquals(fakeUuid, result);
        verify(mockRestTemplate, times(1)).postForEntity(anyString(), any(), eq(String.class));
    }

    @Test
    public void testStoreMessage_non2xxStatus_throwsDescriptiveError() {
        // Arrange
        AcpMessage msg = new AcpMessage();
        msg.setUid("s1234567");
        msg.setKey("ABC");
        msg.setComment("bad");
        msg.setValue(5.0);

        RestTemplate mockRestTemplate = mock(RestTemplate.class);
        AcpStorageService storageService = new AcpStorageService(mockRestTemplate);

        ResponseEntity<String> errorResponse = new ResponseEntity<>("error", HttpStatus.INTERNAL_SERVER_ERROR);
        when(mockRestTemplate.postForEntity(anyString(), any(), eq(String.class)))
                .thenReturn(errorResponse);

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            storageService.storeMessage(msg);
        });

        System.out.println("Actual exception message: " + exception.getMessage());
        assertTrue(exception.getMessage().startsWith("Failed to store message"));
        assertTrue(exception.getMessage().contains("500")); // INTERNAL_SERVER_ERROR = 500

    }
    @Test
    public void testStoreMessage_restTemplateThrows_throwsWrappedError() {
        // Arrange
        AcpMessage msg = new AcpMessage();
        msg.setUid("s1234567");
        msg.setKey("DEF");
        msg.setComment("error");
        msg.setValue(3.14);

        RestTemplate mockRestTemplate = mock(RestTemplate.class);
        AcpStorageService storageService = new AcpStorageService(mockRestTemplate);

        when(mockRestTemplate.postForEntity(anyString(), any(), eq(String.class)))
                .thenThrow(new RuntimeException("Connection timed out"));

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            storageService.storeMessage(msg);
        });

        assertTrue(exception.getMessage().contains("Storage call failed"));
        assertNotNull(exception.getCause());
        assertEquals("Connection timed out", exception.getCause().getMessage());
    }
}