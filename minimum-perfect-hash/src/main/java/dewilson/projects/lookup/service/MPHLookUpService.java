package dewilson.projects.lookup.service;

import com.indeed.mph.TableReader;
import dewilson.projects.lookup.support.SimpleSupport;
import dewilson.projects.lookup.support.Support;

import java.io.IOException;
import java.io.InputStream;

public class MPHLookUpService implements LookUpService {

    private final Support statusSupport;
    private final Support filterSupport;
    private TableReader<String, String> reader;

    public MPHLookUpService() {
        this.statusSupport = new SimpleSupport();
        this.filterSupport = new SimpleSupport();
    }

    @Override
    public boolean idExists(final String id) {
        return this.reader.containsKey(id);
    }

    @Override
    public String getStatus(final String id) {
        if (this.reader.containsKey(id)) {
            try {
                return this.reader.get(id);
            } catch (final IOException ioe) {
                throw new RuntimeException("Error getting status for id");
            }
        } else {
            return "DNE";
        }
    }

    @Override
    public Support getStatusSupport() {
        return this.statusSupport;
    }

    @Override
    public InputStream getFilter(final String type) {
        throw new UnsupportedOperationException("No filters are supported yet for MPH");
    }

    @Override
    public Support getFilterSupport() {
        return this.filterSupport;
    }

    @Override
    public void loadResource(final String resource) throws IOException {
        this.reader = TableReader.open(resource);
        this.reader.spliterator().forEachRemaining(pair -> this.statusSupport.addSupportedType(pair.getSecond()));
        this.statusSupport.addSupportedType("DNE");
    }

    @Override
    public String getType() {
        return "mph-1.0.4";
    }

}