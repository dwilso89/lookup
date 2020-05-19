package dewilson.projects.lookup.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dewilson.projects.lookup.connector.LookUpConnector;
import org.rapidoid.config.Conf;
import org.rapidoid.config.Config;
import org.rapidoid.setup.App;
import org.rapidoid.setup.My;
import org.rapidoid.setup.On;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.ServiceLoader;

public class RapidoidRestServer {

    private static LookUpConnector lookUpConnector;
    private static int CHUNK_TRANSFER_SIZE = 256 * 1024;

    public static void main(final String[] args) {
        App.run(args);

        // configure server...
        final Config serverConf = Conf.section("server");
        if (!serverConf.isEmpty() && serverConf.keys().contains("CHUNK_TRANSFER_SIZE")) {
            CHUNK_TRANSFER_SIZE = Integer.parseInt(serverConf.get("CHUNK_TRANSFER_SIZE").toString());
        }

        // configure LookUp service...
        final Config lookUpConf = Conf.section("lookUp");
        initializeLookUpConnector(lookUpConf);

        // handle errors...
        My.errorHandler((req, resp, error) -> resp.code(500).result("Error handling request"));

        // endpoints...
        On.get("/exists").json((String id) -> lookUpConnector.idExists(id));

        On.get("/getValue").managed(true).json((String id) -> lookUpConnector.getValue(id));

        On.get("/getSupportedFilters").serve(() -> serializeJson(lookUpConnector.getFilterSupport()));

        On.get("/getFilter").plain((req, resp) -> {
            req.async(); // mark asynchronous request processing

            final byte[] bytes = new byte[CHUNK_TRANSFER_SIZE];

            try (final InputStream is = lookUpConnector.getFilter(req.param("type"))) {
                int numRead;
                while (-1 != (numRead = is.read(bytes))) {
                    // if last chunk or partial read for some reason...
                    if (numRead < CHUNK_TRANSFER_SIZE) {
                        resp.chunk(Arrays.copyOf(bytes, numRead));
                    } else {
                        resp.chunk(bytes);
                    }
                }
            }
            return resp.done();
        });

    }

    private static void initializeLookUpConnector(final Config lookUpConf) {
        final String type = lookUpConf.get("serviceType").toString();

        for (final LookUpConnector potentialService : ServiceLoader.load(LookUpConnector.class)) {
            System.out.println("Found service with type " + potentialService.getServiceType());
            if (potentialService.getServiceType().trim().equalsIgnoreCase(type)) {
                lookUpConnector = potentialService;
                break;
            }
        }

        if(lookUpConnector == null){
            throw new RuntimeException("Could not find service " + type);
        }

        long start = System.currentTimeMillis();
        lookUpConnector.initialize(lookUpConf.toFlatMap());
        System.out.println(String.format("Initialization finished in [%d]ms", System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        final Object resource = lookUpConf.get("resource");
        if (resource != null) {
            try {
                lookUpConnector.loadResource(resource.toString());
            } catch (final IOException ioe) {
                throw new RuntimeException("Could not load resource [" + resource + "] with service [" + type + "]", ioe);
            }
        }
        System.out.println(String.format("Resource loading finished in [%d]ms", System.currentTimeMillis() - start));

    }

    private static String serializeJson(final Object o) {
        try {
            return new ObjectMapper().writeValueAsString(o);
        } catch (final JsonProcessingException jpe) {
            throw new RuntimeException(jpe);
        }
    }

}