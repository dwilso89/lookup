package dewilson.projects.lookup.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class SimpleTCPClient {

    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;

    public void startConnection(final String ip, final int port) throws IOException {
        this.clientSocket = new Socket(ip, port);
        this.out = new PrintWriter(clientSocket.getOutputStream(), true);
        this.in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
    }

    public String sendMessage(final String msg) throws IOException {
        this.out.println(msg);
        return in.readLine();
    }

    public void stopConnection() throws IOException {
        this.in.close();
        this.out.close();
        this.clientSocket.close();
    }

}
