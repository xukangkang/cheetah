package org.kk.cheetah.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class CheetahServer {
	public static void main(String[] args) {
		
	}
	public void bind(int port){
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
			e.printStackTrace();
		}
		
	}
	private class ChildChannelHandler extends ChannelInitializer<SocketChannel>{

		@Override
		protected void initChannel(SocketChannel socketChannel) throws Exception {
			socketChannel.pipeline();
		}
		
	}
}
