package org.kk.cheetah.handler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

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

    private Map<ConsumerRecordRequest, ClientConsume> clientConsumeMap = new ConcurrentHashMap<ConsumerRecordRequest, ClientConsume>();

    private AtomicLong al = new AtomicLong();
    public boolean support(ClientRequest clientRequest) {
        return clientRequest instanceof ConsumerRecordRequest;
    }

    public void handle(ClientRequest clientRequest, ChannelHandlerContext ctx) {
        if (logger.isDebugEnabled()) {
            logger.debug("handle ->收到请求, 第  {} 条消息,clientRequest:{}",al.incrementAndGet(), clientRequest);
        }
        
        ConsumerRecordRequest consumerRecordRequest = (ConsumerRecordRequest) clientRequest;
        ConsumerRecords consumerRecords = getConsumerRecords(consumerRecordRequest);
        //发送响应
        ctx.writeAndFlush(consumerRecords);
    }

    private ConsumerRecords getConsumerRecords(ConsumerRecordRequest consumerRecordRequest) {
        ClientConsume clientConsume = null;
        if (!clientConsumeMap.containsKey(consumerRecordRequest)) {
            clientConsumeMap.put(consumerRecordRequest, clientConsume = new ClientConsume(0));
        } else {
            clientConsume = clientConsumeMap.get(consumerRecordRequest);
        }

        return clientConsume.getConsumerRecords(consumerRecordRequest);
    }


}
