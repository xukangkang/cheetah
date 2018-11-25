package org.kk.cheetah.model;

import org.kk.cheetah.common.model.request.ConsumerRecordRequest;
import org.kk.cheetah.common.model.response.ConsumerRecord;
import org.kk.cheetah.common.model.response.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientConsume {
    private Logger logger = LoggerFactory.getLogger(ClientConsume.class);
    private String group;
    private String clientId;
    private long offset;

    private RecordRead recordRead;

    public ClientConsume(long offset) {
        recordRead = new RecordRead(offset);

    }

    public ConsumerRecords getConsumerRecords(ConsumerRecordRequest consumerRecordRequest) {
        ConsumerRecords consumerRecords = new ConsumerRecords();
        consumerRecords.setOnlyTag(consumerRecordRequest.getPollTag());
        for (int index = 0; index < consumerRecordRequest.getMaxPollNum(); index++) {
            ConsumerRecord consumerRecord = null;
            if ((consumerRecord = recordRead.readRecord()) == null) {
                return consumerRecords;
            }
            consumerRecords.add(consumerRecord);
        }
        return consumerRecords;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

}
