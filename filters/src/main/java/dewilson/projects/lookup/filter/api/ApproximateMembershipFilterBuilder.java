package dewilson.projects.lookup.filter.api;

import java.util.Map;
import java.util.stream.Stream;

public interface ApproximateMembershipFilterBuilder {

    String getType();

    ApproximateMembershipFilterBuilder initialize(final Map<String, String> configuration);

    ApproximateMembershipFilterBuilder elements(final Stream<byte[]> elements);

    ApproximateMembershipFilter build();

}
