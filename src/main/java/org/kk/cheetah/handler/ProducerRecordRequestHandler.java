package org.kk.cheetah.handler;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.kk.cheetah.common.model.request.ClientRequest;
import org.kk.cheetah.common.model.request.ProducerRecordRequest;
import org.kk.cheetah.common.model.response.ConsumerRecord;
import org.kk.cheetah.common.model.response.ProducerRecord;
import org.msgpack.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;

public class ProducerRecordRequestHandler extends AbstractHandler {

    private static Logger logger = LoggerFactory.getLogger(ProducerRecordRequestHandler.class);

    private static String dataFilePath = "E:\\cheetah-file\\data.txt";
    
    private final static  BlockingQueue<ConsumerRecord> consumerRecordQueue = new LinkedBlockingQueue<ConsumerRecord>();
    static{
    	new Thread(new Runnable() {
			
			public void run() {
				ConsumerRecord consumerRecord = null;
				while(true){
					doWriteFile(consumerRecordQueue.poll());
				}
			}
		}).start();
    }
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
        consumerRecord.setKey(producerRecordRequest.getKey());
        consumerRecord.setData(producerRecordRequest.getData());
        writeFile(consumerRecord);
    }
    private void writeFile(ConsumerRecord consumerRecord){
    	try {
			consumerRecordQueue.put(consumerRecord);
		} catch (InterruptedException e) {
			logger.error("writeFile",e);
		}
    }
    private static void doWriteFile(ConsumerRecord consumerRecord) {
    	if(consumerRecord == null){
    		return;
    	}
        //MessagePack msgpack = new MessagePack();
        try {
 /*           byte[] raw = msgpack.write(consumerRecord);
            FileOutputStream fos = new FileOutputStream(dataFilePath, true);
            fos.write(raw);
            fos.write('\r');
            fos.flush();
            fos.close();*/
          ObjectOutputStream nongsh = new ObjectOutputStream(new FileOutputStream(dataFilePath, true));
          nongsh.writeObject(consumerRecord);
          nongsh.writeByte('\r');
          nongsh.close();
        } catch (IOException e) {
            logger.error("doWriteFile", e);
        }
    }

}
