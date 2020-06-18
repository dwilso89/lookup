package dewilson.projects.lookup.connector;

import com.google.common.collect.Maps;
import dewilson.projects.lookup.filter.HadoopMembershipFilter;
import dewilson.projects.lookup.reader.CSVKVReader;
import dewilson.projects.lookup.support.DefaultSupportTypes;
import dewilson.projects.lookup.support.SimpleSupport;
import dewilson.projects.lookup.support.Support;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.examples.Expander;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_MAPFILE_BLOOM_SIZE_KEY;

public class BloomMapLookUpConnector implements LookUpConnector {

    private final Support filterSupport;
    private final Configuration conf;
    private BloomMapFile.Reader reader;
    private String bloomFilterLocation;

    public BloomMapLookUpConnector() {
        this.filterSupport = new SimpleSupport(DefaultSupportTypes.FILTER);
        this.conf = new Configuration();
    }

    @Override
    public void initialize(final Map<String, String> config) {
        config.forEach(this.conf::set);
    }

    @Override
    public synchronized boolean keyExists(final String key) {
        final Text textValue = new Text(key);
        try {
            // TODO manage lack of thread safety
            if (this.reader.probablyHasKey(textValue)) {
                return (this.reader.get(textValue, new Text()) != null);
            }
        } catch (final IOException ioe) {
            throw new RuntimeException("Failed to check status of key [" + key + "]");
        }

        return false;
    }

    @Override
    public synchronized String getValue(final String key) {
        final Text textKey = new Text(key);
        try {
            // TODO manage lack of thread safety
            final Text textValue = new Text();
            if (this.reader.get(textKey, textValue) != null) {
                return textValue.toString();
            }
        } catch (final IOException ioe) {
            throw new RuntimeException("Failed to check status of key [" + key + "]");
        }

        return "DNE";
    }

    // TODO get filter to service
    public InputStream getFilter(final String type) throws IOException {
        if (type.equals("dynamic-hadoop-bloommap-2.10.0")) {
            return new FileInputStream(new File(bloomFilterLocation));
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public void loadResource(final String resource) throws IOException {
        final String resourceType = this.conf.get("lookUp.connector.resource.type", "tgz");

        final Path path;
        switch (resourceType) {
            case "tgz":
                path = loadTgz(resource);
                break;
            case "dir":
                path = new Path(resource);
                break;
            case "csv":
                path = loadCsv(resource);
                break;
            default:
                path = new Path(resource);
        }

        this.bloomFilterLocation = path.toString() + "/bloom";
        this.reader = new BloomMapFile.Reader(path, this.conf);

        this.filterSupport.addSupport(HadoopMembershipFilter.TYPE);
    }

    private Path loadTgz(final String resource) throws IOException {
        final File source = new File(resource);
        final File destination = new File(resource.substring(0, resource.lastIndexOf('/')));
        try {
            new Expander().expand(source, destination);
        } catch (final ArchiveException ae) {
            throw new IOException("Could not expand compressed archive at resource location [" + resource + "]", ae);
        }
        return new Path(destination.toString());
    }

    private Path loadCsv(final String resource) throws IOException {
        final Path bloomPath = new Path(this.conf.get("lookUp.work.dir", new File(resource).getAbsolutePath()) + "/bloom");

        this.conf.setLong(IO_MAPFILE_BLOOM_SIZE_KEY, 86400000);

        try (final BloomMapFile.Writer writer = new BloomMapFile.Writer(
                this.conf,
                bloomPath,
                BloomMapFile.Writer.keyClass(Text.class),
                BloomMapFile.Writer.valueClass(Text.class),
                BloomMapFile.Writer.compression(SequenceFile.CompressionType.BLOCK))) {
            final Map<String, String> map = Maps.newHashMap();
            this.conf.forEach((entry) -> map.put(entry.getKey(), entry.getValue()));
            final Text key = new Text();
            final Text value = new Text();
            new CSVKVReader().initialize(resource, map).getKVStream().forEach(pair -> {
                key.set(pair.getKey());
                value.set(pair.getValue());
                try {
                    writer.append(key, value);
                } catch (final IOException ioe) {
                    throw new RuntimeException("Error reading resource file " + resource, ioe);
                }
            });
        }

        return bloomPath;
    }

    @Override
    public String getConnectorType() {
        return "hadoop-bloommap-2.10";
    }

    @Override
    public Stream<String> getAllKeys() {
        final KeyIterator keyIterator = new KeyIterator();
        keyIterator.reset();

        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(new KeyIterator().reset(), Spliterator.ORDERED), false);
    }

    class KeyIterator implements Iterator<String> {

        private final Text key = new Text();
        private final Text value = new Text();
        private boolean hasNext = false;

        KeyIterator reset() {
            try {
                reader.reset();
            } catch (final IOException ioe) {
                throw new RuntimeException("Error resetting inner Hadoop MapFile reader", ioe);
            }
            return this;
        }

        @Override
        public boolean hasNext() {
            if (!hasNext) {
                try {
                    hasNext = reader.next(this.key, this.value);
                } catch (final IOException ioe) {
                    throw new RuntimeException("Error resetting inner Hadoop MapFile reader", ioe);
                }
            }

            return hasNext;
        }

        @Override
        public String next() {
            if (hasNext()) {
                hasNext = false;
                return key.toString();
            }

            throw new NoSuchElementException("No more elements left to read from MapFile.");
        }

    }
}
