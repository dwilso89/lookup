package dewilson.projects.lookup.connector;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.linkedin.paldb.api.PalDB;
import com.linkedin.paldb.api.StoreReader;
import com.linkedin.paldb.api.StoreWriter;
import dewilson.projects.lookup.reader.CSVKVReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PalDBLookUpConnector implements LookUpConnector {

    private final Map<String, String> config;
    private StoreReader[] readers;
    private int partitions;

    public PalDBLookUpConnector() {
        this.config = Maps.newHashMap();
    }

    @Override
    public void initialize(final Map<String, String> config) {
        this.config.putAll(config);
        this.partitions = Integer.parseInt(
                this.config.getOrDefault(
                        Configuration.PARTITION_COUNT_KEY,
                        Configuration.DEFAULT_PARTITION_COUNT));
        this.readers = new StoreReader[this.partitions];

    }

    @Override
    public boolean keyExists(final String key) {
        return !getValue(key).equals("DNE");
    }

    @Override
    public String getValue(final String key) {
        final Object value;
        final StoreReader reader = getStoreReaderForId(key);
        synchronized (reader) {
            value = reader.get(key);
        }
        if (value != null) {
            return value.toString();
        }
        return "DNE";
    }

    private StoreReader getStoreReaderForId(final String key) {
        if (this.partitions > 1) {
            return this.readers[Math.abs(key.hashCode() % this.partitions)];
        } else {
            return this.readers[0];
        }
    }

    @Override
    public void loadResource(final String resource) throws IOException {
        final String resourceType = this.config.getOrDefault("lookUp.connector.resource.type", "palDB");

        switch (resourceType) {
            case "palDB":
                this.readers[0] = PalDB.createReader(new File(resource));
                break;
            case "csv":
                final File palDBFile = new File(this.config.getOrDefault("lookUp.work.dir", "/tmp/") + "/palDB");
                if (palDBFile.exists()) {
                    Files.walk(palDBFile.toPath())
                            .sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(File::delete);
                }

                final List<StoreWriter> writers = Lists.newArrayList();
                if (this.partitions > 1) {
                    for (int i = 0; i < partitions; i++) {
                        writers.add(PalDB.createWriter(new File(palDBFile.getAbsolutePath() + "/partition-" + i + ".store")));
                    }

                    new CSVKVReader().initialize(resource, this.config).getKVStream().forEach(pair -> {
                        final int hash = Math.abs(pair.getKey().hashCode() % this.partitions);
                        writers.get(hash).put(pair.getKey(), pair.getValue());
                    });

                    writers.forEach(StoreWriter::close);

                    for (int i = 0; i < this.partitions; i++) {
                        this.readers[i] = PalDB.createReader(
                                new File(palDBFile.getAbsolutePath() + "/partition-" + i + ".store"));
                    }
                } else {
                    final StoreWriter writer = PalDB.createWriter(palDBFile);
                    new CSVKVReader().initialize(resource, this.config).getKVStream()
                            .forEach(pair -> writer.put(pair.getKey(), pair.getValue()));
                    writer.close();
                    this.readers[0] = PalDB.createReader(palDBFile);
                }
                break;
            default:
                throw new IllegalArgumentException("PalDB does not support resource type [" + resourceType + "]");
        }

    }

    @Override
    public String getConnectorType() {
        return "palDB-1.2.0";
    }

    @Override
    public Stream<String> getAllKeys() {
        Stream<String> keyStream = new ArrayList<String>().stream();
        for (final StoreReader reader : this.readers) {
            keyStream = Streams.concat(keyStream, StreamSupport.stream(reader.keys().spliterator(), false)
                    .map(Object::toString));
        }

        return keyStream;
    }

}