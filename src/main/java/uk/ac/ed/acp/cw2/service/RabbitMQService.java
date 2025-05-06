package uk.ac.ed.acp.cw2.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

@Service
public class RabbitMQService {

    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;

    @Autowired
    public RabbitMQService(RabbitTemplate rabbitTemplate, ObjectMapper objectMapper) {
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
    }

    // Method to send messages to RabbitMQ
    public void sendMessagesToRabbitMQ(String queueName, List<Map<String, Object>> messages) {
        for (Map<String, Object> message : messages) {
            try {
                // Convert the message to JSON string
                String jsonMessage = objectMapper.writeValueAsString(message);

                // Send to RabbitMQ
                rabbitTemplate.convertAndSend(queueName, jsonMessage);
            } catch (Exception e) {
                // Handle exception
            }
        }
    }
}