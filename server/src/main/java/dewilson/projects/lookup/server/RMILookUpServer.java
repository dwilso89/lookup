package dewilson.projects.lookup.server;

import dewilson.projects.lookup.rmi.LookUp;
import dewilson.projects.lookup.service.LookUpService;
import dewilson.projects.lookup.service.LookUpServiceWithFilter;
import org.newsclub.net.unix.rmi.AFUNIXNaming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;

public class RMILookUpServer implements LookUp {

    private static final Logger LOG = LoggerFactory.getLogger(RMILookUpServer.class);
    private LookUpService lookUpService;

    public RMILookUpServer(final Map<String, String> configuration) {
        this.lookUpService = new LookUpServiceWithFilter(configuration);
    }

    public boolean keyExists(String key) {
        return this.lookUpService.keyExists(key);
    }

    public String getValue(String key) {
        return this.lookUpService.getValue(key);
    }

    public static void main(String args[]) {

        final Map<String, String> lookUpConf = new HashMap<>();
        lookUpConf.put("lookUp.connector.type", "palDB-1.2.0");
        lookUpConf.put("lookUp.connector.resource.location", "./data/GOOG.csv");
        lookUpConf.put("lookUp.connector.resource.type", "csv");
        lookUpConf.put("lookUp.work.dir", "../target/");
        lookUpConf.put("lookUp.filters", "scala,guava-29.0,hadoop-2.10");
        lookUpConf.put("lookUp.filter.active.type", "scala");
        lookUpConf.put("lookUp.key.col", "0");
        lookUpConf.put("lookUp.val.col", "4");
        lookUpConf.put("lookUp.partition", "true");
        lookUpConf.put("lookUp.partitions", "4");

        if (args.length > 0 && args[0].trim().equals("RMI")) {

            int port = Integer.valueOf(args[1]);

            try {
                final RMILookUpServer obj = new RMILookUpServer(lookUpConf);
                final LookUp stub = (LookUp) UnicastRemoteObject.exportObject(obj, port);

                // Bind the remote object's stub in the registry
                final Registry registry = LocateRegistry.getRegistry();
                registry.rebind("LookUp", stub);
                LOG.info("RMI Server ready");
            } catch (Exception e) {
                LOG.error("Server exception: ", e);
            }

        } else {

            try {
                final AFUNIXNaming naming = AFUNIXNaming.getInstance();
                naming.createRegistry();
                // naming.setRemoteShutdownAllowed(false);
                LOG.info("Using {}", naming.getSocketFactory());

                final RMILookUpServer obj = new RMILookUpServer(lookUpConf);
                LOG.info("Binding {} to 'LookUp'...", obj);
                naming.exportAndBind("LookUp", obj);

                LOG.info("Ready to accept connections!");
            } catch (Exception e) {
                LOG.error("Server exception: ", e);
            }
        }

    }
}