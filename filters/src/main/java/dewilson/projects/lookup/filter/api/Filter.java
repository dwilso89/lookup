package dewilson.projects.lookup.filter.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface Filter<T> {

    String getType();

//    void initialize(final Map<String, String> configuration);

    void write(final OutputStream os) throws IOException;

    Filter<T> read(final InputStream is) throws IOException;

}
