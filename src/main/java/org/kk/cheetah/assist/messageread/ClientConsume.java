package org.kk.cheetah.assist.messageread;

import org.kk.cheetah.common.model.request.ConsumerRecordRequest;
import org.kk.cheetah.common.model.response.ConsumerRecord;
import org.kk.cheetah.common.model.response.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientConsume {
    private Logger logger = LoggerFactory.getLogger(ClientConsume.class);
    private String group;
    private String clientId;
    private volatile long offset;
    private RecordRead recordRead;

    public ClientConsume(long offset, String topic) {
        this.offset = offset;
        recordRead = new RecordRead(offset, topic);

    }

    //同步，防止并发访问时读文件出错
    public synchronized ConsumerRecords getConsumerRecords(ConsumerRecordRequest consumerRecordRequest) {
        ConsumerRecords consumerRecords = new ConsumerRecords();
        consumerRecords.setOnlyTag(consumerRecordRequest.getPollTag());
        System.out.println("maxPollNum " + consumerRecordRequest.getMaxPollNum());
        for (int index = 0; index < consumerRecordRequest.getMaxPollNum(); index++, offset++) {
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
