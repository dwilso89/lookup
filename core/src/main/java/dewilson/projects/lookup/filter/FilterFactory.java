package dewilson.projects.lookup.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Stream;

public final class FilterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(FilterFactory.class);

    private FilterFactory() {
        // empty on purpose
    }

    public static MembershipFilter getMembershipFilter(final String type, final Map<String, String> configuration) {
        LOG.debug("Configuration: ");
        configuration.entrySet().forEach(entry -> LOG.debug("Key: {}  Value: {}", entry.getKey(), entry.getValue()));

        return buildMembershipFilter(getFilterBuilder(type).initialize(configuration));
    }

    public static MembershipFilter getMembershipFilter(final String type, final Map<String, String> configuration, final Stream<String> keyStream) {
        return buildMembershipFilter(getFilterBuilder(type).initialize(configuration).elements(keyStream));
    }

    private static MembershipFilter buildMembershipFilter(final MembershipFilterBuilder membershipFilterBuilder) {
        return membershipFilterBuilder.build();
    }

    private static MembershipFilterBuilder getFilterBuilder(final String type) {
        for (final MembershipFilterBuilder membershipFilterBuilder : ServiceLoader.load(MembershipFilterBuilder.class)) {
            LOG.debug("Found filter builder with type [{}]", membershipFilterBuilder.getType());
            if (membershipFilterBuilder.getType().trim().equalsIgnoreCase(type)) {
                LOG.debug("Returning filter builder with type [{}]", membershipFilterBuilder.getType());
                return membershipFilterBuilder;
            }
        }
        throw new RuntimeException(String.format("Could not find FilterBuilder with type [%s]", type));
    }

}
