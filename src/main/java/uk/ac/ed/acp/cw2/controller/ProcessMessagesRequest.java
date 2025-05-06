package uk.ac.ed.acp.cw2.controller;


import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ProcessMessagesRequest {
    private String readTopic;
    private String writeQueueGood;
    private String writeQueueBad;
    private int messageCount;
}
