package uk.ac.ed.acp.cw2.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TransformedMessage {
    private String key;
    private int version;
    private double value;
}
