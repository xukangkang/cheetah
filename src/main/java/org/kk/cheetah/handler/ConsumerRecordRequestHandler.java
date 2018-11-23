package org.kk.cheetah.handler;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.kk.cheetah.common.model.request.ClientRequest;
import org.kk.cheetah.common.model.request.ConsumerRecordRequest;
import org.kk.cheetah.common.model.response.ConsumerRecord;
import org.kk.cheetah.common.model.response.ConsumerRecords;
import org.kk.cheetah.model.ClientConsume;
import org.msgpack.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;

public class ConsumerRecordRequestHandler extends AbstractHandler {

    private Logger logger = LoggerFactory.getLogger(ConsumerRecordRequestHandler.class);

    private String dataFilePath = "E:\\cheetah-file\\data.txt";

    private Map<ConsumerRecordRequest, ClientConsume> clientConsumeMap = new ConcurrentHashMap<>();

    @Override
    public boolean support(ClientRequest clientRequest) {
        return clientRequest instanceof ConsumerRecordRequest;
    }

    @Override
    public void handle(ClientRequest clientRequest, ChannelHandlerContext ctx) {
        if (logger.isDebugEnabled()) {
            logger.debug("handle", clientRequest);
        }
        ConsumerRecordRequest consumerRecordRequest = (ConsumerRecordRequest) clientRequest;
        logger.debug("handle -> 收到请求:{}", consumerRecordRequest);
        //发送响应
        ctx.writeAndFlush(getConsumerRecords(consumerRecordRequest));
    }

    private ConsumerRecords getConsumerRecords(ConsumerRecordRequest consumerRecordRequest) {
        ClientConsume clientConsume = null;
        if (!clientConsumeMap.containsKey(consumerRecordRequest)) {
            clientConsumeMap.put(consumerRecordRequest, clientConsume = new ClientConsume(0));
        } else {
            clientConsume = clientConsumeMap.get(consumerRecordRequest);
        }

        return clientConsume.getConsumerRecords(consumerRecordRequest.getMaxPollNum());
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
