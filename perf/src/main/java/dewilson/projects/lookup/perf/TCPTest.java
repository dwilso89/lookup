package dewilson.projects.lookup.perf;

import dewilson.projects.lookup.client.tcp.SimpleTCPClient;

import java.io.IOException;

public class TCPTest {

    public static void main(String[] args) throws IOException {

        final SimpleTCPClient client = new SimpleTCPClient();
        client.startConnection("localhost", 8888);
        final long start = System.currentTimeMillis();

        String response = "";
         for(int i = 0; i < 100000; i++) {
            response  = client.sendMessage("2018-05-05");
        }
        System.out.println(System.currentTimeMillis() - start);
        client.stopConnection();
        System.out.println("Response: " + response);

    }

}