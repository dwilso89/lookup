package dewilson.projects.lookup.server;

import dewilson.projects.lookup.connector.LookUpConnector;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

class TCPLookUpServer {

    public static void main(final String[] args) {
        try {
            final Map<String, String> lookUpConf = new HashMap<>();
            lookUpConf.put("port", "8888");
            lookUpConf.put("lookUp.serviceType", "palDB-1.2.0");
            lookUpConf.put("lookUp.resource", "./data/GOOG.csv");
            lookUpConf.put("lookUp.resourceType", "csv");
            lookUpConf.put("lookUp.work.dir", "../target/");
            lookUpConf.put("lookUp.filter.guavaBloom", " true");
            lookUpConf.put("lookUp.filter.scalaBloom", "true");
            lookUpConf.put("lookUp.filter.activeType", "scalaBloom");
            lookUpConf.put("lookUp.key.col", "0");
            lookUpConf.put("lookUp.val.col", "4");
            lookUpConf.put("lookUp.partition", "true");
            lookUpConf.put("lookUp.partitions", "4");
            initializeLookUpConnector(lookUpConf);
            final TCPLookUpServer server = new TCPLookUpServer();
            server.start(lookUpConf, initializeLookUpConnector(lookUpConf));
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static LookUpConnector initializeLookUpConnector(final Map<String, String> lookUpConf) {
        final String type = lookUpConf.get("lookUp.serviceType");

        LookUpConnector lookUpConnector = null;

        for (final LookUpConnector potentialService : ServiceLoader.load(LookUpConnector.class)) {
            System.out.println("Found service with type " + potentialService.getServiceType());
            if (potentialService.getServiceType().trim().equalsIgnoreCase(type)) {
                lookUpConnector = potentialService;
                break;
            }
        }

        if (lookUpConnector == null) {
            throw new RuntimeException("Could not find service " + type);
        }

        long start = System.currentTimeMillis();
        lookUpConnector.initialize(lookUpConf);
        System.out.println(String.format("Initialization finished in [%d]ms", System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        final Object resource = lookUpConf.get("lookUp.resource");
        if (resource != null) {
            try {
                lookUpConnector.loadResource(resource.toString());
            } catch (final IOException ioe) {
                throw new RuntimeException("Could not load resource [" + resource + "] with service [" + type + "]", ioe);
            }
        }
        System.out.println(String.format("Resource loading finished in [%d]ms", System.currentTimeMillis() - start));

        return lookUpConnector;
    }

    private LookUpConnector lookUpConnector;

    void start(final Map<String, String> config, final LookUpConnector lookUpConnector) throws InterruptedException {
        this.lookUpConnector = lookUpConnector;
        final int port = Integer.parseInt(config.get("port"));
        System.out.println("Starting server at: " + port);
        final EventLoopGroup bossGroup = new NioEventLoopGroup();
        final EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            final ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new TCPChannelInitializer())
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            final ChannelFuture f = b.bind(port).sync();
            if (f.isSuccess()) System.out.println("Server started successfully");
            f.channel().closeFuture().sync();
        } finally {
            System.out.println("Stopping server");
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
            System.out.println(String.format("< %s > : Channel Active", ctx.channel().remoteAddress()));
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final String s) {
            final String result = lookUpConnector.idExists(s.trim()) ? "true\n" : "false\n";
            ctx.channel().writeAndFlush(result);
        }

        @Override
        public void channelInactive(final ChannelHandlerContext ctx) {
            System.out.println(String.format("< %s > : Channel Inactive", ctx.channel().remoteAddress()));
        }
    }


}
