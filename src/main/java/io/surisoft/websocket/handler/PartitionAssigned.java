package io.surisoft.websocket.handler;

import java.time.Instant;

public class PartitionAssigned {
    private String clientId;
    private Integer partition;
    private Instant assignedOn;

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Instant getAssignedOn() {
        return assignedOn;
    }

    public void setAssignedOn(Instant assignedOn) {
        this.assignedOn = assignedOn;
    }
}
