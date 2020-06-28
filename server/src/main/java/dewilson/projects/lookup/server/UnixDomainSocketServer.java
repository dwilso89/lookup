package dewilson.projects.lookup.server;

import dewilson.projects.lookup.service.LookUpServiceWithFilter;
import org.newsclub.net.unix.AFUNIXServerSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


class UnixDomainSocketServer {

    private static final Logger LOG = LoggerFactory.getLogger(UnixDomainSocketServer.class);

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

            new UnixDomainSocketServer(configuration);

        } catch (final IOException e) {
            e.printStackTrace();
        }

    }

    private LookUpServiceWithFilter lookUpServiceWithFilter;

    private UnixDomainSocketServer(final Map<String, String> configuration) throws IOException {
        this.lookUpServiceWithFilter = new LookUpServiceWithFilter(configuration);

        final File socketFile = new File(new File(System.getProperty("java.io.tmpdir")), "junixsocket-test.sock");
        System.out.println(socketFile);

        try (AFUNIXServerSocket server = AFUNIXServerSocket.newInstance()) {
            // server.setReuseAddress(false);
            server.bind(new AFUNIXSocketAddress(socketFile));
            System.out.println("server: " + server);

            while (!Thread.interrupted()) {
                System.out.println("Waiting for connection...");
                try (final Socket sock = server.accept()) {
                    System.out.println("Connected: " + sock);
                    long lookUpTime = 0;
                    long socketRead = 0;
                    long socketWrite = 0;
                    try (final InputStream is = sock.getInputStream();
                         final DataOutputStream dos = new DataOutputStream(sock.getOutputStream())) {
                        String key = "";
                        final byte[] buf = new byte[512];
                        while (!Thread.interrupted()) {
                            long currentTimeNano = System.nanoTime();
                            final int read = is.read(buf);
                            if(read == -1){
                                System.out.println("read length -1. last key: " + key);
                                System.out.println(key.equals("break"));
                                break;
                            }

                            key = new String(buf, 0, read, "UTF-8");
                            socketRead += System.nanoTime() - currentTimeNano;

                            if (key.equals("break")) {
                                System.out.println("Encountered 'break' key");
                                break;
                            }
                            currentTimeNano = System.nanoTime();
                            final boolean response = this.lookUpServiceWithFilter.keyExists(key);
                            lookUpTime += System.nanoTime() - currentTimeNano;

                            currentTimeNano = System.nanoTime();
                            dos.writeBoolean(response);
                            socketWrite += System.nanoTime() - currentTimeNano;

                            dos.flush();
                        }
                    }
                    System.out.println("Disconnected: " + sock);
                    System.out.println("Time reading socket " + TimeUnit.MILLISECONDS.convert(socketRead, TimeUnit.NANOSECONDS));
                    System.out.println("Time writing socket " + TimeUnit.MILLISECONDS.convert(socketWrite, TimeUnit.NANOSECONDS));
                    System.out.println("Time total in lookup " + TimeUnit.MILLISECONDS.convert(lookUpTime, TimeUnit.NANOSECONDS));
                }

            }
        }
    }

}