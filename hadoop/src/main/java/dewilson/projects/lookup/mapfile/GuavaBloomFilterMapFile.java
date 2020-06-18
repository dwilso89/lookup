package dewilson.projects.lookup.mapfile;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class GuavaBloomFilterMapFile {
    private static final Logger LOG = LoggerFactory.getLogger(GuavaBloomFilterMapFile.class);

    public static class Writer extends MapFile.Writer {
        private BloomFilter<String> filter;
        private FileSystem fs;
        private Path dir;

        public Writer(Configuration conf, Path dir, SequenceFile.Writer.Option... options) throws IOException {
            super(conf, dir, options);
            this.fs = dir.getFileSystem(conf);
            this.dir = dir;
            initFilter(conf);
        }

        private synchronized void initFilter(Configuration conf) {
            final long expectedElements = conf.getLong("bloom.filter.expected.elements", 10000000L);
            final double fpp = conf.getDouble("bloom.filter.fpp", 0.005F);
            this.filter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), expectedElements, fpp);
        }

        @Override
        public synchronized void append(WritableComparable key, Writable val)
                throws IOException {
            super.append(key, val);
            this.filter.put(key.toString());
        }

        @Override
        public synchronized void close() throws IOException {
            super.close();
            try (DataOutputStream out = fs.create(new Path(dir, "bloom"), true)) {
                this.filter.writeTo(out);
            }
        }

    }

    public static class Reader extends MapFile.Reader {
        private BloomFilter<String> filter;

        public Reader(Path dir, Configuration conf, SequenceFile.Reader.Option... options) throws IOException {
            super(dir, conf, options);
            initBloomFilter(dir, conf);
        }

        private void initBloomFilter(Path dirName, Configuration conf) {
            try {
                FileSystem fs = dirName.getFileSystem(conf);
                try (DataInputStream in = fs.open(new Path(dirName, "bloom"))) {
                    // read filter
                    this.filter = BloomFilter.readFrom(in, Funnels.stringFunnel(Charsets.UTF_8));
                }
            } catch (IOException ioe) {
                LOG.warn("Can't open filter: " + ioe + " - fallback to MapFile.");
                this.filter = null;
            }
        }

        public boolean probablyHasKey(WritableComparable key) throws IOException {
            if (this.filter == null) {
                return true;
            }
            return this.filter.mightContain(key.toString());
        }

        public BloomFilter<String> getBloomFilter() {
            return this.filter;
        }

        @Override
        public synchronized Writable get(WritableComparable key, Writable val) throws IOException {
            if (!probablyHasKey(key)) {
                return null;
            }
            return super.get(key, val);
        }
    }
}
