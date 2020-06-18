package dewilson.projects.lookup.filter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface MembershipFilter {

    String getType();

    FilterResult test(String key);

    void write(final OutputStream os) throws IOException;

    MembershipFilter read(final InputStream is) throws IOException;

}
