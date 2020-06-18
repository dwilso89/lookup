package dewilson.projects.lookup.mapfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.LongStream;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_MAPFILE_BLOOM_SIZE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScalaBloomFilterMapFileTest {


    @Test
    void testScalaBloomFilterMapFileCreation(@TempDir Path tempDir) throws Exception {
        final org.apache.hadoop.fs.Path bloomPath = new org.apache.hadoop.fs.Path(tempDir.toString() + "/bloomSmall");

        try (final ScalaBloomFilterMapFile.Writer writer = new ScalaBloomFilterMapFile.Writer(new Configuration(),
                bloomPath,
                BloomMapFile.Writer.keyClass(Text.class),
                BloomMapFile.Writer.valueClass(Text.class))) {
            writer.append(new Text("a"), new Text("PUBLIC"));
            writer.append(new Text("b"), new Text("PRIVATE"));
            writer.append(new Text("c"), new Text("REDACT"));
            writer.append(new Text("d"), new Text("PUBLIC"));
            writer.append(new Text("e"), new Text("PRIVATE"));
            writer.append(new Text("f"), new Text("REDACT"));
        }

        final ScalaBloomFilterMapFile.Reader reader = new ScalaBloomFilterMapFile.Reader(bloomPath, new Configuration());

        assertTrue(reader.probablyHasKey(new Text("a")));
        assertTrue(reader.probablyHasKey(new Text("b")));
        assertTrue(reader.probablyHasKey(new Text("c")));
        assertTrue(reader.probablyHasKey(new Text("d")));
        assertTrue(reader.probablyHasKey(new Text("e")));

        final Text key = new Text();
        final Text value = new Text();

        key.set("a");
        assertEquals(reader.get(key, value).toString(), "PUBLIC");

        key.set("b");
        assertEquals(reader.get(key, value).toString(), "PRIVATE");

        key.set("c");
        reader.get(key, value);
        assertEquals(reader.get(key, value).toString(), "REDACT");

        key.set("DNE");
        assertNull(reader.get(key, value));
    }


    @Test
    void generateMapFiles() throws Exception {
        final int elements = 10000000;
        final Configuration configuration = new Configuration();
        configuration.setInt(IO_MAPFILE_BLOOM_SIZE_KEY, elements);
        configuration.setLong("bloom.filter.expected.elements", elements);

        final BloomMapFile.Writer hadoopBloomMapFileWriter = new BloomMapFile.Writer(
                configuration,
                new org.apache.hadoop.fs.Path("/tmp/hadoop-bloom-mapfile/"),
                BloomMapFile.Writer.keyClass(LongWritable.class),
                BloomMapFile.Writer.valueClass(NullWritable.class));

        final GuavaBloomFilterMapFile.Writer guavaBloomFilterMapFileWriter = new GuavaBloomFilterMapFile.Writer(
                configuration,
                new org.apache.hadoop.fs.Path("/tmp/guava-bloom-mapfile/"),
                BloomMapFile.Writer.keyClass(LongWritable.class),
                BloomMapFile.Writer.valueClass(NullWritable.class),
                BloomMapFile.Writer.compression(SequenceFile.CompressionType.NONE));


        final ScalaBloomFilterMapFile.Writer scalaBloomFilterMapFileWriter = new ScalaBloomFilterMapFile.Writer(
                configuration,
                new org.apache.hadoop.fs.Path("/tmp/scala-bloom-mapfile/"),
                BloomMapFile.Writer.keyClass(LongWritable.class),
                BloomMapFile.Writer.valueClass(NullWritable.class));

        final LongWritable key = new LongWritable();
        LongStream.range(0L, elements).forEach(l -> {
            try {
                key.set(l);
                guavaBloomFilterMapFileWriter.append(key, NullWritable.get());
                scalaBloomFilterMapFileWriter.append(key, NullWritable.get());
                hadoopBloomMapFileWriter.append(key, NullWritable.get());
            } catch (final IOException ioe) {
                throw new RuntimeException(ioe);
            }
        });
        hadoopBloomMapFileWriter.close();
        guavaBloomFilterMapFileWriter.close();
        scalaBloomFilterMapFileWriter.close();
    }
}
