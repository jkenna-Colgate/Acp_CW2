package uk.ac.ed.acp.cw2.controller;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service
public class KafkaMessageReader {

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    public List<String> readMessagesFromBeginning(String topic, int messageCount) {
        List<String> messages = new ArrayList<>();
        try (Consumer<String, String> consumer = consumerFactory.createConsumer()) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100)); // force assignment
            consumer.seekToBeginning(consumer.assignment());

            while (messages.size() < messageCount) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    messages.add(record.value());
                    if (messages.size() >= messageCount) {
                        break;
                    }
                }
            }
        }
        return messages;
    }
}