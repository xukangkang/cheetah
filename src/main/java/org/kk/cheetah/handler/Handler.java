package org.kk.cheetah.handler;

import org.kk.cheetah.common.model.request.ClientRequest;

import io.netty.channel.ChannelHandlerContext;

public interface Handler {
    void handle(ClientRequest clientRequest, ChannelHandlerContext ctx);

    boolean support(ClientRequest clientRequest);
}
