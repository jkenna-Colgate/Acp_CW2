package uk.ac.ed.acp.cw2.service;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import uk.ac.ed.acp.cw2.model.AcpMessage;

@Service
public class AcpStorageService {

    private static final String DEFAULT_STORAGE_URL = "https://acp-storage.azurewebsites.net";

    private final RestTemplate restTemplate;
    private final String storageUrl;

    public AcpStorageService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
        String url = System.getenv("ACP_STORAGE_SERVICE");
        this.storageUrl = (url != null) ? url : DEFAULT_STORAGE_URL;
    }

    public String storeMessage(AcpMessage message) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<AcpMessage> entity = new HttpEntity<>(message, headers);

        ResponseEntity<String> response;
        try {
            response = restTemplate.postForEntity(
                    storageUrl + "/api/v1/blob", entity, String.class
            );
        } catch (Exception e) {
            System.out.println("Caught exception in ACP call: " + e.getMessage());
            throw new RuntimeException("Storage call failed", e);
        }

        if (response.getStatusCode().is2xxSuccessful()) {
            String rawBody = response.getBody();
            return rawBody != null && rawBody.startsWith("\"") && rawBody.endsWith("\"")
                    ? rawBody.substring(1, rawBody.length() - 1)  // remove quotes
                    : rawBody;
        } else {
            System.out.println("Throwing due to non-2xx: " + response.getStatusCode());
            throw new RuntimeException("Failed to store message: " + response.getStatusCode());
        }

    }
}