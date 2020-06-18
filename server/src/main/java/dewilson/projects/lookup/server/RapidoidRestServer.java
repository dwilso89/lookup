package dewilson.projects.lookup.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dewilson.projects.lookup.service.LookUpServiceWithFilter;
import org.rapidoid.config.Conf;
import org.rapidoid.config.Config;
import org.rapidoid.setup.App;
import org.rapidoid.setup.My;
import org.rapidoid.setup.On;

import java.io.InputStream;
import java.util.Arrays;

public class RapidoidRestServer {

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
        final LookUpServiceWithFilter lookUpServiceWithFilter = new LookUpServiceWithFilter(lookUpConf.toFlatMap());

        // handle errors...
        My.errorHandler((req, resp, error) -> resp.code(500).result("Error handling request"));

        // endpoints...
        On.get("/exists").json((String id) -> lookUpServiceWithFilter.keyExists(id));

        On.get("/getValue").managed(true).json((String id) -> lookUpServiceWithFilter.getValue(id));

        On.get("/getSupportedFilters").serve(() -> serializeJson(lookUpServiceWithFilter.getFilterSupport()));

        On.get("/getFilter").plain((req, resp) -> {
            req.async(); // mark asynchronous request processing

            final byte[] bytes = new byte[CHUNK_TRANSFER_SIZE];

            try (final InputStream is = lookUpServiceWithFilter.getFilter(req.param("type"))) {
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

    private static String serializeJson(final Object o) {
        try {
            return new ObjectMapper().writeValueAsString(o);
        } catch (final JsonProcessingException jpe) {
            throw new RuntimeException(jpe);
        }
    }

}
