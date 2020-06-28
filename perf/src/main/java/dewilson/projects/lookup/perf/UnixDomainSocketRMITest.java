package dewilson.projects.lookup.perf;


import dewilson.projects.lookup.rmi.LookUp;
import org.newsclub.net.unix.rmi.AFUNIXNaming;
import org.newsclub.net.unix.rmi.RemotePeerInfo;

import java.rmi.registry.Registry;

public class UnixDomainSocketRMITest {

    private UnixDomainSocketRMITest() {
    }

    public static void main(String[] args) {
        try {
            final AFUNIXNaming naming = AFUNIXNaming.getInstance();

            System.out.println("Locating registry...");
            final Registry registry = naming.getRegistry();
            System.out.println(registry);
            System.out.println();

            final LookUp lookUp = (LookUp) registry.lookup("LookUp");
            System.out.println("LookUP instance:");
            System.out.println("    " + lookUp);
            System.out.println("    " + RemotePeerInfo.remotePeerCredentials(lookUp));
            System.out.println();

            final long start = System.currentTimeMillis();

            for (int i = 0; i < 1000000; i++) {
                lookUp.keyExists("2020-05-05");
            }

            System.out.println("Time elapsed: " + (System.currentTimeMillis() - start) + " ms");
        } catch (Exception e) {
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
        }
    }
}