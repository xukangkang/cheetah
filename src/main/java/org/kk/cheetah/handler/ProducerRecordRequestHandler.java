package org.kk.cheetah.handler;

import java.io.FileOutputStream;
import java.io.IOException;

import org.kk.cheetah.common.model.request.ClientRequest;
import org.kk.cheetah.common.model.request.ProducerRecordRequest;
import org.kk.cheetah.common.model.response.ConsumerRecord;
import org.kk.cheetah.common.model.response.ProducerRecord;
import org.msgpack.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;

public class ProducerRecordRequestHandler extends AbstractHandler {

    private Logger logger = LoggerFactory.getLogger(ProducerRecordRequestHandler.class);

    private String dataFilePath = "E:\\cheetah-file\\data.txt";

    public boolean support(ClientRequest clientRequest) {
        return clientRequest instanceof ProducerRecordRequest;
    }

    public void handle(ClientRequest clientRequest, ChannelHandlerContext ctx) {
        if (logger.isDebugEnabled()) {
            logger.debug("handle", clientRequest);
        }
        ProducerRecordRequest producerRecordRequest = (ProducerRecordRequest) clientRequest;
        logger.debug("handle -> 收到数据:{}", producerRecordRequest);
        //发送响应
        ProducerRecord producerRecord = new ProducerRecord();
        producerRecord.setDataId(producerRecordRequest.getDataId());
        ctx.writeAndFlush(producerRecord);
        //写入磁盘
        ConsumerRecord consumerRecord = new ConsumerRecord();
        consumerRecord.setKey(consumerRecord.getKey());
        consumerRecord.setData(producerRecordRequest.getData());
        //TODO 异步写文件
        writeFile(consumerRecord);
    }

    private void writeFile(ConsumerRecord consumerRecord) {
        MessagePack msgpack = new MessagePack();
        try {
            byte[] raw = msgpack.write(consumerRecord);
            FileOutputStream fos = new FileOutputStream(dataFilePath, true);
            fos.write(raw);
            fos.write('\r');
            fos.flush();
            fos.close();
        } catch (IOException e) {
            logger.error("writeFile", e);
        }
    }

}
