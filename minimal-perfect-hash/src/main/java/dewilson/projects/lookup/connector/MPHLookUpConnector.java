package dewilson.projects.lookup.connector;

import com.google.common.collect.Maps;
import com.indeed.mph.TableConfig;
import com.indeed.mph.TableReader;
import com.indeed.mph.TableWriter;
import com.indeed.mph.serializers.SmartStringSerializer;
import com.indeed.util.core.Pair;
import dewilson.projects.lookup.reader.CSVKVReader;
import dewilson.projects.lookup.support.DefaultSupportTypes;
import dewilson.projects.lookup.support.SimpleSupport;
import dewilson.projects.lookup.support.Support;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.stream.Stream;

public class MPHLookUpConnector implements LookUpConnector {

    private final Support filterSupport;
    private final Map<String, String> config;
    private TableReader<String, String> reader;


    public MPHLookUpConnector() {
        this.filterSupport = new SimpleSupport(DefaultSupportTypes.FILTER);
        this.config = Maps.newHashMap();
    }

    @Override
    public void initialize(final Map<String, String> config) {
        this.config.putAll(config);
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
    public InputStream getFilter(final String type) {
        throw new UnsupportedOperationException("No filters are supported yet for MPH");
    }

    @Override
    public Support getFilterSupport() {
        return this.filterSupport;
    }

    @Override
    public void loadResource(final String resource) throws IOException {
        final String resourceType = this.config.getOrDefault("lookUp.resourceType", "mph");

        final String mphFile;
        switch (resourceType) {
            case "mph":
                mphFile = resource;
                break;
            case "csv":
                mphFile = this.config.getOrDefault("lookUp.work.dir", "/tmp") + "/mph";
                final TableConfig<String, String> tableConfig = new TableConfig<String, String>()
                        .withKeySerializer(new SmartStringSerializer())
                        .withValueSerializer(new SmartStringSerializer());

                final Stream<Pair<String, String>> pairStream = new CSVKVReader()
                        .initialize(resource, this.config)
                        .getKVStream()
                        .map(pair -> new Pair<>(pair.getKey(), pair.getValue()));

                TableWriter.writeWithTempStorage(new File(mphFile), tableConfig, pairStream.iterator());
                break;
            default:
                throw new IllegalArgumentException("MPH does not support resource type [" + resourceType + "]");
        }
        this.reader = TableReader.open(mphFile);
    }

    @Override
    public String getServiceType() {
        return "mph-1.0.4";
    }

}