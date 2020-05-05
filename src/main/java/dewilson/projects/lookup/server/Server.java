package dewilson.projects.lookup.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dewilson.projects.lookup.service.LookUpService;
import org.rapidoid.config.Conf;
import org.rapidoid.config.Config;
import org.rapidoid.setup.App;
import org.rapidoid.setup.My;
import org.rapidoid.setup.On;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.ServiceLoader;

public class Server {

    private static LookUpService lookUpService;
    private static int CHUNKED_TRANSFER_SIZE = 256 * 1024;

    public static void main(final String[] args) {
        App.run(args);

        // configure server...
        final Config serverConf = Conf.section("server");
        if (!serverConf.isEmpty() && serverConf.keys().contains("CHUNK_TRANSFER_SIZE")) {
            CHUNKED_TRANSFER_SIZE = Integer.parseInt(serverConf.get("CHUNK_TRANSFER_SIZE").toString());
        }

        // configure LookUp service...
        final Config lookUpConf = Conf.section("lookUp");
        initializeLookUpService(lookUpConf);

        // handle errors...
        My.errorHandler((req, resp, error) -> resp.code(500).result("Error handling request"));

        // endpoints...
        On.get("/exists").json((String id) -> lookUpService.getStatus(id));

        On.get("/getSupportedStatuses").serve(() -> serializeJson(lookUpService.getStatusSupport()));

        On.get("/getStatus").json((String id) -> lookUpService.getStatus(id));

        On.get("/getSupportedFilters").serve(() -> serializeJson(lookUpService.getFilterSupport()));

        On.get("/getFilter").plain((req, resp) -> {
            req.async(); // mark asynchronous request processing

            final byte[] bytes = new byte[CHUNKED_TRANSFER_SIZE];

            try (final InputStream is = lookUpService.getFilter(req.param("type"))) {
                int numRead;
                while (-1 != (numRead = is.read(bytes))) {
                    // if last chunk or partial read for some reason...
                    if (numRead < CHUNKED_TRANSFER_SIZE) {
                        resp.chunk(Arrays.copyOf(bytes, numRead));
                    } else {
                        resp.chunk(bytes);
                    }
                }
            }
            return resp.done();
        });

    }

    private static void initializeLookUpService(final Config lookUpConf) {
        final String type = lookUpConf.get("serviceType").toString();

        for (final LookUpService potentialService : ServiceLoader.load(LookUpService.class)) {
            if (potentialService.getType().trim().equalsIgnoreCase(type)) {
                lookUpService = potentialService;
                break;
            }
        }

        final Object resource = lookUpConf.get("resource");
        if (resource != null) {
            try {
                lookUpService.loadResource(resource.toString());
            } catch (final IOException ioe) {
                throw new RuntimeException("Could not load resource [" + resource + "] with service [" + type + "]", ioe);
            }
        }
    }

    private static String serializeJson(final Object o) {
        try {
            return new ObjectMapper().writeValueAsString(o);
        } catch (final JsonProcessingException jpe) {
            throw new RuntimeException(jpe);
        }
    }

}
