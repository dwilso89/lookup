package dewilson.projects.lookup.filter;

import java.util.Map;
import java.util.stream.Stream;

public interface MembershipFilterBuilder<T extends MembershipFilter> {

    String getType();

    MembershipFilterBuilder<T> initialize(final Map<String, String> configuration);

    MembershipFilterBuilder<T> elements(final Stream<String> elements);

    T build();

}
