package org.kk.cheetah.handler;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.kk.cheetah.assist.messagewrite.MessageWriteResult;
import org.kk.cheetah.assist.messagewrite.MessageWriteResult.MessageWriteResultEnum;
import org.kk.cheetah.assist.messagewrite.MessageWriter;
import org.kk.cheetah.assist.messagewrite.QueueMessageWriter;
import org.kk.cheetah.common.model.request.ClientRequest;
import org.kk.cheetah.common.model.request.ProducerRecordRequest;
import org.kk.cheetah.common.model.response.ConsumerRecord;
import org.kk.cheetah.common.model.response.ProducerRecord;
import org.kk.cheetah.serializer.MarshallingSerializer;
import org.kk.cheetah.serializer.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;

public class ProducerRecordRequestHandler extends AbstractHandler {

    private static Logger logger = LoggerFactory.getLogger(ProducerRecordRequestHandler.class);

    private MessageSerializer messageSerializer;
    private MessageWriter messageWriter;

    public ProducerRecordRequestHandler() {
        messageSerializer = new MarshallingSerializer();
        messageWriter = new QueueMessageWriter();
    }

    @Override
    public boolean support(ClientRequest clientRequest) {
        return clientRequest instanceof ProducerRecordRequest;
    }

    @Override
    public void handle(ClientRequest clientRequest, ChannelHandlerContext ctx) {
        if (logger.isDebugEnabled()) {
            logger.debug("handle", clientRequest);
        }
        ProducerRecordRequest producerRecordRequest = (ProducerRecordRequest) clientRequest;
        logger.debug("handle -> 收到生产者请求:{}", producerRecordRequest);

        //写入磁盘
        ConsumerRecord consumerRecord = new ConsumerRecord();
        consumerRecord.setKey(producerRecordRequest.getKey());
        consumerRecord.setData(producerRecordRequest.getData());
        consumerRecord.setTopic(producerRecordRequest.getTopic());
        Future<MessageWriteResult> writeFileFuture = messageWriter.writeFile(consumerRecord);
        ProducerRecord producerRecord = new ProducerRecord();
        //发送响应
        producerRecord.setDataId(producerRecordRequest.getDataId());
        try {
            if (writeFileFuture.get().getResult().equals(MessageWriteResultEnum.FAIL)) {
                producerRecord.setErrorMsg("produce message fail");
            }
        } catch (InterruptedException e) {
            logger.error("handle", e);
            producerRecord.setErrorMsg("produce message fail");
        } catch (ExecutionException e) {
            logger.error("handle", e);
            producerRecord.setErrorMsg("produce message fail");
        }
        ctx.writeAndFlush(producerRecord);

    }

}
