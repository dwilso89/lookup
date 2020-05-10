package dewilson.projects.lookup.service;

import com.indeed.mph.TableReader;
import dewilson.projects.lookup.support.DefaultSupportTypes;
import dewilson.projects.lookup.support.SimpleSupport;
import dewilson.projects.lookup.support.Support;

import java.io.IOException;
import java.io.InputStream;

public class MPHLookUpService implements LookUpService {

    private final Support valueSupport;
    private final Support filterSupport;
    private TableReader<String, String> reader;

    public MPHLookUpService() {
        this.valueSupport = new SimpleSupport(DefaultSupportTypes.VALUE);
        this.filterSupport = new SimpleSupport(DefaultSupportTypes.FILTER);
    }

    @Override
    public boolean idExists(final String id) {
        return this.reader.containsKey(id);
    }

    @Override
    public String getValue(final String id) {
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
    public Support getValueSupport() {
        return this.valueSupport;
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
        this.reader.spliterator().forEachRemaining(pair -> this.valueSupport.addSupport(pair.getSecond()));
        this.valueSupport.addSupport("DNE");
    }

    @Override
    public String getServiceType() {
        return "mph-1.0.4";
    }

}