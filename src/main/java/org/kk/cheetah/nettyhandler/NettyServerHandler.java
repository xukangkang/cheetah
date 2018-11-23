package org.kk.cheetah.nettyhandler;

import org.kk.cheetah.common.model.request.ClientRequest;
import org.kk.cheetah.handler.Handler;
import org.kk.cheetah.handler.HandlerSelector;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

public class NettyServerHandler extends ChannelHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ClientRequest) {
            ClientRequest clientRequest = (ClientRequest) msg;
            Handler handler = HandlerSelector.select(clientRequest);
            handler.handle(clientRequest, ctx);
        } else {
            //TODO 返回给客户端不支持请求
            throw new RuntimeException("不支持请求");
        }
    }
}
