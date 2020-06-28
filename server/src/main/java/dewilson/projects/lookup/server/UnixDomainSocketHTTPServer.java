package dewilson.projects.lookup.server;

import dewilson.projects.lookup.service.LookUpServiceWithFilter;
import fi.iki.elonen.NanoHTTPD;
import org.newsclub.net.unix.AFUNIXServerSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;


class UnixDomainSocketHTTPServer extends NanoHTTPD {

    private static final Logger LOG = LoggerFactory.getLogger(UnixDomainSocketHTTPServer.class);

    public static void main(final String[] args) {
        try {

            final Map<String, String> configuration = new HashMap<>();
            configuration.put("port", "8888");
            configuration.put("lookUp.connector.type", "palDB-1.2.0");
            configuration.put("lookUp.connector.resource.location", "./data/GOOG.csv");
            configuration.put("lookUp.connector.resource.type", "csv");
            configuration.put("lookUp.work.dir", "../target/");
            configuration.put("lookUp.filters", "scala,guava-29.0,hadoop-2.10");
            configuration.put("lookUp.filter.active.type", "scala");
            configuration.put("lookUp.key.col", "0");
            configuration.put("lookUp.val.col", "4");
            configuration.put("lookUp.partition", "true");
            configuration.put("lookUp.partitions", "4");
            new UnixDomainSocketHTTPServer(configuration,
                    new AFUNIXSocketAddress(new File("/tmp/junixsocket-http-server.sock")) //
            ).start(NanoHTTPD.SOCKET_READ_TIMEOUT, false);

        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    private LookUpServiceWithFilter lookUpServiceWithFilter;

    private UnixDomainSocketHTTPServer(final Map<String, String> configuration, final AFUNIXSocketAddress socketAddress) throws IOException {
        super(0);
        this.lookUpServiceWithFilter = new LookUpServiceWithFilter(configuration);

        setServerSocketFactory(new ServerSocketFactory() {

            @Override
            public ServerSocket create() throws IOException {
                return AFUNIXServerSocket.forceBindOn(socketAddress);
            }

        });

        LOG.info("Address: {}", socketAddress);
        LOG.info("Try:  curl --unix-socket {} http://localhost/", socketAddress.getPath());
    }

    @Override
    public Response serve(final IHTTPSession session) {
        if (session.getMethod() == Method.GET) {
            if (session.getParameters() != null) {
                if (session.getParameters().get("key") != null) {
                    for (final String key : session.getParameters().get("key")) {
                        return newFixedLengthResponse(String.valueOf(this.lookUpServiceWithFilter.keyExists(key)));
                    }
                }
            }
        }

        return newFixedLengthResponse("Hello world\n");
    }

}
