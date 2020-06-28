package dewilson.projects.lookup.perf;

import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;

import java.io.DataInputStream;
import java.io.File;
import java.io.OutputStream;
import java.net.SocketException;

public class UnixDomainSocketTest {

    public static void main(final String[] args) throws Exception {
        final File socketFile = new File(new File(System.getProperty("java.io.tmpdir")), "junixsocket-test.sock");

        try (final AFUNIXSocket sock = AFUNIXSocket.newInstance()) {
            try {
                sock.connect(new AFUNIXSocketAddress(socketFile));
            } catch (SocketException e) {
                System.out.println("Cannot connect to server. Have you started it?\n");
                throw e;
            }
            System.out.println("Connected");

            try (final DataInputStream dis = new DataInputStream(sock.getInputStream());
                 final OutputStream os = sock.getOutputStream()) {
                final int total = 1000000;
                final long start = System.currentTimeMillis();
                byte[] keyBytes = "2020-05-04".getBytes();
                for (int i = 0; i < total; i++) {

                    os.write(keyBytes);
                    os.flush();

                   // System.out.println("Now reading response from the server...");
                    boolean response = dis.readBoolean();
                   // System.out.println("Response: " + response);

                    if(i % 50000 == 0){
                        System.out.println(response);
                        System.out.println("Total complete: " + ((float)i / total) * 100 + "%");
                    }
                }
                os.write("break".getBytes());
                os.flush();
                System.out.println("Total time : " + (System.currentTimeMillis() - start) + "ms");
            }
        }

        System.out.println("End of communication.");
    }

}
