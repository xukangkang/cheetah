package org.kk.cheetah.handler;

import org.kk.cheetah.common.model.request.ClientRequest;

public class HandlerSelector {
    private static Handler[] handlers = new Handler[] { new ProducerRecordRequestHandler(),
            new ConsumerRecordRequestHandler() };

    public static Handler select(ClientRequest clientRequest) {
        for (Handler handler : handlers) {
            if (handler.support(clientRequest)) {
                return handler;
            }
        }
        //TODO 自定义异常
        throw new RuntimeException("没有找到匹配的处理器");
    }
}
