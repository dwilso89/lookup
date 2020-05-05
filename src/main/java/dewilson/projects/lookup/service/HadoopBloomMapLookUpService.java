package dewilson.projects.lookup.service;

import dewilson.projects.lookup.support.SimpleSupport;
import dewilson.projects.lookup.support.Support;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.examples.Expander;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public class HadoopBloomMapLookUpService implements LookUpService {

    private final Support statusSupport;
    private final Support filterSupport;
    private final Configuration conf;
    private final Text key;
    private final Text value;
    private BloomMapFile.Reader reader;

    public HadoopBloomMapLookUpService() {
        this.statusSupport = new SimpleSupport();
        this.filterSupport = new SimpleSupport();
        this.conf = new Configuration();
        this.key = new Text();
        this.value = new Text();
    }

    @Override
    public void initialize(final Map<String, String> config) {
        config.forEach(this.conf::set);
    }

    @Override
    public String getStatus(final String id) {
        this.key.set(id);
        try {
            if (this.reader.probablyHasKey(this.key)) {
                if (this.reader.get(this.key, this.value) != null) {
                    return this.value.toString();
                }
            }
        } catch (final IOException ioe) {
            throw new RuntimeException("Failed to check status of id [" + id + "]");
        }

        return "DNE";
    }

    @Override
    public Support getStatusSupport() {
        return this.statusSupport;
    }

    @Override
    public OutputStream getFilter(final String type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Support getFilterSupport() {
        return this.filterSupport;
    }

    @Override
    public void loadResource(final String resource) throws IOException {
        final Path path;

        if (resource.endsWith("tgz")) {
            final File source = new File(resource);
            final File destination = new File(resource.substring(0, resource.lastIndexOf('/')));
            try {
                new Expander().expand(source, destination);
            } catch (final ArchiveException ae) {
                throw new IOException("Could not expand archive at resource location [" + resource + "]", ae);
            }
            path = new Path(destination.toString());
        } else {
            path = new Path(resource);
        }

        this.reader = new BloomMapFile.Reader(path, this.conf);
    }

    @Override
    public String getType() {
        return "hadoop-bloommap-2.10";
    }
}