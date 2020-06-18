package dewilson.projects.lookup.connector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.ServiceLoader;

public class LookUpConnectorFactory {

    private static final Logger LOG = LoggerFactory.getLogger(LookUpConnectorFactory.class);

    private LookUpConnectorFactory() {
        // empty on purpose
    }

    public static LookUpConnector getLookUpConnector(final Map<String, String> configuration) {

        LOG.debug("Configuration: ");
        configuration.entrySet().forEach(entry -> LOG.debug("Key: {}  Value: {}", entry.getKey(), entry.getValue()));

        final String type = configuration.get("lookUp.connector.type");

        final LookUpConnector lookUpConnector = getLookUpConnector(type);

        long start = System.currentTimeMillis();
        lookUpConnector.initialize(configuration);
        LOG.info("Initialization finished in [{}]ms", System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        final Object resource = configuration.get("lookUp.connector.resource.location");
        if (resource != null) {
            try {
                lookUpConnector.loadResource(resource.toString());
            } catch (final IOException ioe) {
                throw new RuntimeException("Could not load resource [" + resource + "] with service [" + type + "]", ioe);
            }
        }
        LOG.info("Resource loading finished in [{}]ms", System.currentTimeMillis() - start);

        return lookUpConnector;
    }

    private static LookUpConnector getLookUpConnector(final String type) {

        for (final LookUpConnector potentialService : ServiceLoader.load(LookUpConnector.class)) {
            LOG.info("Found service with type [{}]", potentialService.getConnectorType());
            if (potentialService.getConnectorType().trim().equalsIgnoreCase(type)) {
                return potentialService;
            }
        }

        throw new RuntimeException(String.format("Could not find LookUpConnector with type [%s]", type));
    }

}
