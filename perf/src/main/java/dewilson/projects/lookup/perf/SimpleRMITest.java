package dewilson.projects.lookup.perf;


import dewilson.projects.lookup.rmi.LookUp;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class SimpleRMITest {

    private SimpleRMITest() {
    }

    public static void main(String[] args) {

        final String host = (args.length < 1) ? null : args[0];
        try {
            final Registry registry = LocateRegistry.getRegistry(host);
            final LookUp lookUp = (LookUp) registry.lookup("LookUp");

            final long start = System.currentTimeMillis();

            for (int i = 0; i < 1000000; i++) {
                lookUp.keyExists("2020-05-05");
            }

            System.out.println("Time elapsed: " + (System.currentTimeMillis() - start) + " ms");
            /*
            System.out.println("response: " + stub.exists("2020-04-05"));
            System.out.println("response: " + stub.exists("2020-05-05"));

            System.out.println("response: " + stub.exists("2019-04-05"));
            System.out.println("response: " + stub.exists("2019-05-05"));
            */

        } catch (Exception e) {
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
        }
    }
}