package dewilson.projects.lookup.service;

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
import java.nio.file.Files;
import java.util.Map;

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
        final Path path;

        if (resource.endsWith("tgz")) {
            path = loadTgz(resource);
        } else if (new File(resource).isDirectory()) {
            path = new Path(resource);
        } else if (resource.endsWith("csv")) {
            path = loadCsv(resource);
        } else {
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
    }

    private Path loadTgz(final String resource) throws IOException {
        final File source = new File(resource);
        final File destination = new File(resource.substring(0, resource.lastIndexOf('/')));
        try {
            new Expander().expand(source, destination);
        } catch (final ArchiveException ae) {
            throw new IOException("Could not expand archive at resource location [" + resource + "]", ae);
        }
        return new Path(destination.toString());
    }

    private Path loadCsv(final String resource) throws IOException {
        final File resourceFile = new File(resource);
        final Path bloomPath = new Path(this.conf.get("lookUp.work.dir", resourceFile.getParent().toString()) + "/bloom");

        final int keyCol = this.conf.getInt("lookUp.key.col", 0);
        final int valCol = this.conf.getInt("lookUp.val.col", 1);

        try (final BloomMapFile.Writer writer = new BloomMapFile.Writer(
                this.conf,
                bloomPath,
                BloomMapFile.Writer.keyClass(Text.class),
                BloomMapFile.Writer.valueClass(Text.class),
                BloomMapFile.Writer.compression(SequenceFile.CompressionType.BLOCK))) {
            final Text key = new Text();
            final Text value = new Text();
            Files.lines(resourceFile.toPath()).forEach(line -> {
                final String[] lineSplit = line.split(",");
                key.set(lineSplit[keyCol]);
                value.set(lineSplit[valCol]);
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
