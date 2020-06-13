package dewilson.projects.lookup.filter.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public interface Filter<T> {

    String getType();

    void write(final OutputStream os) throws IOException;

    Filter<T> read(final InputStream is) throws IOException;

}
