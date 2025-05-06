package uk.ac.ed.acp.cw2.data;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TransformRequest {
    private String readQueue;
    private String writeQueue;
    private int messageCount;
}