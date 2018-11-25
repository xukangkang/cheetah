package org.kk.cheetah.model;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

import org.kk.cheetah.common.model.response.ConsumerRecord;
import org.msgpack.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordRead {
    private Logger logger = LoggerFactory.getLogger(ClientConsume.class);
    private String dataFilePath = "E:\\cheetah-file\\data.txt";
    private FileInputStream fis = null;

    public RecordRead(long offset) {
        try {
            fis = new FileInputStream(dataFilePath);
        } catch (FileNotFoundException e) {
            logger.error("RecordRead", e);
        }
        //如果offset大于0，将跳到指定的行
        if (offset > 0) {
            skip();
        }
    }

    private void skip() {
    }

    public ConsumerRecord readRecord() {
        int length = -1;
        byte[] oneByte = new byte[1];
    	try {
    		List<Byte> list = new ArrayList<Byte>();
            while ((length = fis.read(oneByte)) != -1 && oneByte[0] != '\r') {
                list.add(oneByte[0]);
            }
            if(list.size() <= 0){
            	return null;
            }
            byte[] consumerRecordBytes = new byte[list.size()];
            for (int index = 0; index < list.size(); index++) {
                consumerRecordBytes[index] = list.get(index);
            }
            ByteArrayInputStream byteIn = new ByteArrayInputStream(consumerRecordBytes);
            ObjectInputStream objIn = new ObjectInputStream(byteIn);  
			return (ConsumerRecord) objIn.readObject();
		} catch (FileNotFoundException e) {
			logger.error("readRecord", e);
		} catch (IOException e) {
			logger.error("readRecord", e);
		}catch (ClassNotFoundException e) {
			logger.error("readRecord", e);
		}
    	throw new RuntimeException("readRecord exception");
/*        int length = -1;
        byte[] oneByte = new byte[1];
        try {
        	
            List<Byte> list = new ArrayList<Byte>();
            while ((length = fis.read(oneByte)) != -1 && oneByte[0] != '\r') {
                list.add(oneByte[0]);
            }
            byte[] consumerRecordBytes = new byte[list.size()];
            for (int index = 0; index < list.size(); index++) {
                consumerRecordBytes[index] = list.get(index);
            }
            if (consumerRecordBytes.length <= 0) {
                return null;
            }
            MessagePack msgpack = new MessagePack();
            try {
                return msgpack.read(consumerRecordBytes, ConsumerRecord.class);
            } catch (IOException e) {
                logger.error("readRecord", e);
            }
        } catch (IOException e) {
            logger.error("readRecord", e);
        }*/
        
    }

}
