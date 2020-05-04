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
import java.util.ServiceLoader;

public class Server {

    private static LookUpService lookUpService;

    public static void main(final String[] args) {
        App.run(args);

        final Config lookUpConf = Conf.section("lookUp");
        initializeLookUpService(lookUpConf);

        My.errorHandler((req, resp, error) -> resp.code(500).result("Error handling request"));

        On.get("/getSupportedStatuses").serve(() -> serializeJson(lookUpService.getStatusSupport()));

        On.get("/getStatus").json((String id) -> lookUpService.getStatus(id));

        On.get("/getSupportedFilters").serve(() -> serializeJson(lookUpService.getFilterSupport()));

        On.get("/getFilter").serve((String type) -> lookUpService.getFilter(type));

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
