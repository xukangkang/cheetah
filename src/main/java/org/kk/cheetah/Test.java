package org.kk.cheetah;

import java.io.IOException;

import org.kk.cheetah.common.model.response.ConsumerRecord;
import org.msgpack.MessagePack;

public class Test {
    public static void main(String[] args) throws IOException {
        MessagePack msgpack = new MessagePack();
        ConsumerRecord consumerRecord = new ConsumerRecord();
        consumerRecord.setData("zhangsan");
        byte[] raw = msgpack.write(consumerRecord);
        System.out.println(msgpack.read(raw, ConsumerRecord.class));
    }
}
