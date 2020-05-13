package dewilson.projects.lookup.service;

import com.google.common.collect.Maps;
import dewilson.projects.lookup.reader.CSVKVReader;
import dewilson.projects.lookup.reader.KVReader;
import dewilson.projects.lookup.support.DefaultSupportTypes;
import dewilson.projects.lookup.support.SimpleSupport;
import dewilson.projects.lookup.support.Support;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.examples.Expander;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.rapidoid.pool.Pools;

import java.io.*;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_MAPFILE_BLOOM_SIZE_KEY;

public class BloomMapLookUpService implements LookUpService {

    private final Support valueSupport;
    private final Support filterSupport;
    private final Configuration conf;
    private BloomMapFile.Reader reader;
    private String bloomFilterLocation;

    public BloomMapLookUpService() {
        this.valueSupport = new SimpleSupport(DefaultSupportTypes.VALUE);
        this.filterSupport = new SimpleSupport(DefaultSupportTypes.FILTER);
        this.conf = new Configuration();
    }

    @Override
    public void initialize(final Map<String, String> config) {
        config.forEach(this.conf::set);
    }

    @Override
    public boolean idExists(final String id) {
        final Text key = new Text(id);
        try {
            // TODO manage lack of thread safety
            if (this.reader.probablyHasKey(key)) {
                return (this.reader.get(key, new Text()) != null);
            }
        } catch (final IOException ioe) {
            throw new RuntimeException("Failed to check status of id [" + id + "]");
        }

        return false;
    }

    @Override
    public String getValue(final String id) {
        final Text key = new Text(id);
        try {
            if (this.reader.probablyHasKey(key)) {
                final Text value = new Text();
                if (this.reader.get(key, value) != null) {
                    return value.toString();
                }
            }
        } catch (final IOException ioe) {
            throw new RuntimeException("Failed to check status of id [" + id + "]");
        }

        return "DNE";
    }

    @Override
    public Support getValueSupport() {
        return this.valueSupport;
    }

    @Override
    public InputStream getFilter(final String type) throws IOException {
        if (type.equals("dynamic-hadoop-bloommap-2.10.0")) {
            return new FileInputStream(new File(bloomFilterLocation));
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public Support getFilterSupport() {
        return this.filterSupport;
    }

    @Override
    public void loadResource(final String resource) throws IOException {
        final String resourceType = this.conf.get("lookUp.resourceType", "tgz");

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

        if (this.conf.getBoolean("lookup.load.values", true)) {
            final Text key = new Text();
            final Text value = new Text();
            while (this.reader.next(key, value)) {
                this.valueSupport.addSupport(value.toString());
            }
        }

        this.filterSupport.addSupport("dynamic-hadoop-bloommap-2.10.0");
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
    public String getServiceType() {
        return "hadoop-bloommap-2.10";
    }
}
