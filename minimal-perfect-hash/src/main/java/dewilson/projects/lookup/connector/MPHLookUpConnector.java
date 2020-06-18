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
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
    public boolean keyExists(final String key) {
        return this.reader.containsKey(key);
    }

    @Override
    public String getValue(final String key) {
        if (this.reader.containsKey(key)) {
            try {
                return this.reader.get(key);
            } catch (final IOException ioe) {
                throw new RuntimeException("Error getting status for key");
            }
        } else {
            return "DNE";
        }
    }

    @Override
    public void loadResource(final String resource) throws IOException {
        final String resourceType = this.config.getOrDefault("lookUp.connector.resource.type", "mph");

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
    public String getConnectorType() {
        return "mph-1.0.4";
    }

    @Override
    public Stream<String> getAllKeys() {
        return StreamSupport.stream(this.reader.spliterator(), false).map(Pair::getFirst);
    }


}