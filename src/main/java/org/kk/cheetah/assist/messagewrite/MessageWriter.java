package org.kk.cheetah.assist.messagewrite;

import java.util.concurrent.Future;

import org.kk.cheetah.common.model.response.ConsumerRecord;

public interface MessageWriter {
    public Future<MessageWriteResult> writeFile(ConsumerRecord consumerRecord);
}
