package org.kk.cheetah.assist.messageread;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.TimeUnit;

import org.kk.cheetah.common.model.response.ConsumerRecord;
import org.kk.cheetah.config.ServerConfig;
import org.kk.cheetah.serializer.MarshallingSerializer;
import org.kk.cheetah.serializer.MessageSerializer;
import org.kk.cheetah.util.ByteIntConvertUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordRead {
    private Logger logger = LoggerFactory.getLogger(ClientConsume.class);
    private RandomAccessFile randomAccessFile = null;
    private MessageSerializer messageSerializer = new MarshallingSerializer();

    public RecordRead(long offset, String topic) {
        try {
            randomAccessFile = new RandomAccessFile(ServerConfig.dataFilePath + topic, "rw");
        } catch (FileNotFoundException e) {
            logger.error("RecordRead", e);
        }
        //如果offset大于0，将跳到指定的行
        if (offset > 0) {
            skip(offset);
        }
    }

    private void skip(long offset) {

        int readLength = -1;
        try {
            byte[] dataLengthBytes = new byte[4];
            for (int index = 0; index < offset; index++) {
                readLength = randomAccessFile.read(dataLengthBytes);
                if (readLength > -1) {
                    int dataLength = ByteIntConvertUtil.byteArrayToInt(dataLengthBytes);
                    randomAccessFile.skipBytes(dataLength);
                }
            }
        } catch (FileNotFoundException e) {
            logger.error("readRecord", e);
        } catch (IOException e) {
            logger.error("readRecord", e);
        }
    }

    public ConsumerRecord readRecord() {
        int readLength = -1;
        try {
            byte[] dataLengthBytes = new byte[4];
            readLength = randomAccessFile.read(dataLengthBytes);
            if (readLength > -1) {
                int dataLength = ByteIntConvertUtil.byteArrayToInt(dataLengthBytes);
                byte[] dataBytes = new byte[dataLength];
                while (randomAccessFile.read(dataBytes) == -1) {
                    //有可能只写了数据长度，还没有来得及写数据，稍等一纳秒后重试
                    try {
                        TimeUnit.NANOSECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        logger.error("readRecord", e);
                    }
                }
                return messageSerializer.deserialize(dataBytes);
            }
            return null;
        } catch (FileNotFoundException e) {
            logger.error("readRecord", e);
        } catch (IOException e) {
            logger.error("readRecord", e);
        } catch (ClassNotFoundException e) {
            logger.error("readRecord", e);
        }
        throw new RuntimeException("readRecord exception");
    }

}
