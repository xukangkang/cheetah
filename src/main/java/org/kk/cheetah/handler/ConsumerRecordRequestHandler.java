package org.kk.cheetah.handler;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.kk.cheetah.common.model.request.ClientRequest;
import org.kk.cheetah.common.model.request.ConsumerRecordRequest;
import org.kk.cheetah.common.model.response.ConsumerRecords;
import org.kk.cheetah.config.ServerConfig;
import org.kk.cheetah.model.ClientConsume;
import org.kk.cheetah.zookeeper.ZKMetadataHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;

public class ConsumerRecordRequestHandler extends AbstractHandler {

    private static Logger logger = LoggerFactory.getLogger(ConsumerRecordRequestHandler.class);

    private Map<ConsumerRecordRequest, ClientConsume> clientConsumeMap = new ConcurrentHashMap<ConsumerRecordRequest, ClientConsume>();

    public ConsumerRecordRequestHandler() {
    }

    @Override
    public boolean support(ClientRequest clientRequest) {
        return clientRequest instanceof ConsumerRecordRequest;
    }

    @Override
    public void handle(ClientRequest clientRequest, ChannelHandlerContext ctx) {
        if (logger.isDebugEnabled()) {
            logger.debug("handle ->收到消费者请求,clientRequest:{}", clientRequest);
        }

        ConsumerRecordRequest consumerRecordRequest = (ConsumerRecordRequest) clientRequest;
        ConsumerRecords consumerRecords = getConsumerRecords(consumerRecordRequest);
        //发送响应
        ctx.writeAndFlush(consumerRecords);
    }

    private ConsumerRecords getConsumerRecords(ConsumerRecordRequest consumerRecordRequest) {
        ClientConsume clientConsume = null;
        //根据消费者信息得到offset，如果没有对应的offset，则返回null，
        String offsetKey = buildConsumerOffsetKey(consumerRecordRequest);
        Object data = null;
        Long offset = (data = ZKMetadataHandler.getData(offsetKey)) == null ? null : Long.valueOf(data.toString());
        if (!clientConsumeMap.containsKey(consumerRecordRequest)) {
            //如果offset为null，则创建一个offsetKey，值offset初始化为0
            if (offset == null) {
                if (!existTopic(consumerRecordRequest.getTopic())) {
                    ConsumerRecords consumerRecords = new ConsumerRecords();
                    consumerRecords.setOnlyTag(consumerRecordRequest.getPollTag());
                    consumerRecords.setErrorMsg("topic不存在：" + consumerRecordRequest.getTopic());
                    return consumerRecords;
                }
                offset = 0l;
                ZKMetadataHandler.createPersistent(offsetKey, offset);
            }
            clientConsumeMap.put(consumerRecordRequest,
                    clientConsume = new ClientConsume(offset, consumerRecordRequest.getTopic()));
        } else {
            clientConsume = clientConsumeMap.get(consumerRecordRequest);
        }
        ConsumerRecords consumerRecords = clientConsume.getConsumerRecords(consumerRecordRequest);
        //更新offset
        ZKMetadataHandler.update(offsetKey, offset + consumerRecords.getConsumberRecords().size());
        return consumerRecords;
    }

    private boolean existTopic(String topic) {
        File file = new File(ServerConfig.dataFilePath + topic);
        return file.exists();
    }

    private String buildConsumerOffsetKey(ConsumerRecordRequest consumerRecordRequest) {
        return new StringBuffer()
                .append(ZKMetadataHandler.cheetahZKPrefix)
                .append("/topics")
                .append("/").append(consumerRecordRequest.getTopic())
                .append("/partions")
                .append("/").append("1")
                .append("/offset")
                .append("/").append(consumerRecordRequest.getGroup())
                .toString();
    }

}
