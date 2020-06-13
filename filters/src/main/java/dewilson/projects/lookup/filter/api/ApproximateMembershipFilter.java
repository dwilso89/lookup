package dewilson.projects.lookup.filter.api;

import java.io.IOException;
import java.io.InputStream;

public interface ApproximateMembershipFilter extends Filter<byte[]> {

    boolean probablyExists(final byte[] member);

    ApproximateMembershipFilter read(final InputStream is) throws IOException;
}
