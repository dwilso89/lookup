package dewilson.projects.lookup.filter.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Stream;

public class FilterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(FilterFactory.class);

    private FilterFactory() {
        // empty on purpose
    }

    public static ApproximateMembershipFilter getApproximateMembershipFilter(final String type, final Map<String, String> configuration) {
        LOG.debug("Configuration: ");
        configuration.entrySet().forEach(entry -> LOG.debug("Key: {}  Value: {}", entry.getKey(), entry.getValue()));

        return getFilterBuilder(type).initialize(configuration).build();
    }

    public static ApproximateMembershipFilter getApproximateMembershipFilter(final String type, final Map<String, String> configuration, final Stream<byte[]> keyStream) {
        return getFilterBuilder(type).initialize(configuration).elements(keyStream).build();
    }

    private static ApproximateMembershipFilterBuilder getFilterBuilder(final String type) {
        for (final ApproximateMembershipFilterBuilder approximateMembershipFilterBuilder : ServiceLoader.load(ApproximateMembershipFilterBuilder.class)) {
            LOG.info("Found filter builder with type [{}]", approximateMembershipFilterBuilder.getType());
            if (approximateMembershipFilterBuilder.getType().trim().equalsIgnoreCase(type)) {
                return approximateMembershipFilterBuilder;
            }
        }
        throw new RuntimeException(String.format("Could not find FilterBuilder with type [%s]", type));

    }

}
