package org.kk.cheetah.assist.messagewrite;

public class MessageWriteResult {
    private MessageWriteResultEnum result;

    public MessageWriteResultEnum getResult() {
        return result;
    }

    public void setResult(MessageWriteResultEnum result) {
        this.result = result;
    }

    public static enum MessageWriteResultEnum {
        SUCCESS,
        FAIL;
    }
}
