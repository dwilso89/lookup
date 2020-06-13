package dewilson.projects.lookup.server;

import dewilson.projects.lookup.api.connector.LookUpConnector;
import dewilson.projects.lookup.api.connector.LookUpConnectorFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

class TCPLookUpServer {
    
    private static final Logger LOG = LoggerFactory.getLogger(TCPLookUpServer.class);

    public static void main(final String[] args) {
        try {
            final Map<String, String> lookUpConf = new HashMap<>();
            lookUpConf.put("port", "8888");
            lookUpConf.put("lookUp.connector.type", "palDB-1.2.0");
            lookUpConf.put("lookUp.connector.resource", "./data/GOOG.csv");
            lookUpConf.put("lookUp.connector.resource.type", "csv");
            lookUpConf.put("lookUp.work.dir", "../target/");
            lookUpConf.put("lookUp.filters", " scala,guava-29.0,hadoop-2.10");
            lookUpConf.put("lookUp.filter.active.type", "scala");
            lookUpConf.put("lookUp.key.col", "0");
            lookUpConf.put("lookUp.val.col", "4");
            lookUpConf.put("lookUp.partition", "true");
            lookUpConf.put("lookUp.partitions", "4");
            final TCPLookUpServer server = new TCPLookUpServer();
            server.start(lookUpConf, LookUpConnectorFactory.getLookUpConnector(lookUpConf));
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
    }

    private LookUpConnector lookUpConnector;

    void start(final Map<String, String> config, final LookUpConnector lookUpConnector) throws InterruptedException {
        this.lookUpConnector = lookUpConnector;
        final int port = Integer.parseInt(config.get("port"));
        LOG.info("Starting server at [{}]", port);
        final EventLoopGroup bossGroup = new NioEventLoopGroup();
        final EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            final ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new TCPChannelInitializer())
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            final ChannelFuture f = b.bind(port).sync();
            if (f.isSuccess()) LOG.info("Server started successfully");
            f.channel().closeFuture().sync();
        } finally {
            LOG.info("Stopping server");
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    private class TCPChannelInitializer extends ChannelInitializer<SocketChannel> {

        protected void initChannel(final SocketChannel socketChannel) {
            socketChannel.pipeline().addLast(new StringEncoder());
            socketChannel.pipeline().addLast(new StringDecoder());
            socketChannel.pipeline().addLast(new TCPChannelHandler());
        }
    }

    private class TCPChannelHandler extends SimpleChannelInboundHandler<String> {

        @Override
        public void channelActive(final ChannelHandlerContext ctx) {
            LOG.info("Channel Active: [{}]", ctx.channel().remoteAddress());
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final String s) {
            final String result = lookUpConnector.idExists(s.trim()) ? "true\n" : "false\n";
            ctx.channel().writeAndFlush(result);
        }

        @Override
        public void channelInactive(final ChannelHandlerContext ctx) {
            LOG.info(String.format("Channel Inactive: [{}]", ctx.channel().remoteAddress()));
        }
    }


}
