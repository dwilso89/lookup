package dewilso.projects.lookup.filter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface BloomFilter<T> {

    boolean probablyExists(T t);

    void write(final OutputStream os) throws IOException;

    BloomFilter<T> read(final InputStream is) throws IOException;

}
