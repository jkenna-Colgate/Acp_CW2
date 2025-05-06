package uk.ac.ed.acp.cw2.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SummaryMessage {
    private int totalMessagesWritten;
    private int totalMessagesProcessed;
    private int totalRedisUpdates;
    private double totalValueWritten;
    private double totalAdded;
}
