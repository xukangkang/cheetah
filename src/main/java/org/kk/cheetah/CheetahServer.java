package org.kk.cheetah;

import org.kk.cheetah.common.serializable.MarshallingCodeCFactory;
import org.kk.cheetah.handler.ProducerRecordRequestHandler;
import org.kk.cheetah.nettyhandler.NettyServerHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class CheetahServer {
    private Logger logger = LoggerFactory.getLogger(ProducerRecordRequestHandler.class);

    public static void main(String[] args) {
        int port = 9997;
        if (args != null && args.length > 0) {
            try {
                Integer.valueOf(args[0]);
            } catch (Exception e) {
            }
        }
        new CheetahServer().bind(port);
    }

    public void bind(int port) {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childHandler(new ChildChannelHandler());
        try {
            serverBootstrap.bind(port).sync();
        } catch (InterruptedException e) {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }

    }

    private class ChildChannelHandler extends ChannelInitializer<SocketChannel> {

        @Override
        protected void initChannel(SocketChannel socketChannel) throws Exception {
            // 添加Jboss的序列化，编解码工具
            socketChannel.pipeline().addLast(
                    MarshallingCodeCFactory.buildMarshallingEncoder());
            socketChannel.pipeline().addLast(
                    MarshallingCodeCFactory.buildMarshallingDecoder());
            socketChannel.pipeline().addLast(new NettyServerHandler());
        }

    }
}