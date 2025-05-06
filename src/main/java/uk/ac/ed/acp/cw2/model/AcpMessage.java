package uk.ac.ed.acp.cw2.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AcpMessage {
    private String uid;
    private String key;
    private String comment;
    private double value;

    // Optional fields added during processing
    private Double runningTotalValue;
    private String uuid;
}