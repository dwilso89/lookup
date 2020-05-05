package dewilson.projects.lookup.hadoop;

import org.apache.commons.compress.archivers.examples.Archiver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BloomMapFileTest {

    private static String bloomMapFileResource;

    @BeforeAll
    public static void createBloomMapFileArchive(@TempDir Path tempDir) throws Exception {
        final org.apache.hadoop.fs.Path bloomPath = new org.apache.hadoop.fs.Path(tempDir.toString());

        try (final BloomMapFile.Writer writer = new BloomMapFile.Writer(new Configuration(), bloomPath, BloomMapFile.Writer.keyClass(Text.class), BloomMapFile.Writer.valueClass(Text.class))) {
            writer.append(new Text("a"), new Text("PUBLIC"));
            writer.append(new Text("b"), new Text("PRIVATE"));
            writer.append(new Text("c"), new Text("REDACT"));
            writer.append(new Text("d"), new Text("PUBLIC"));
            writer.append(new Text("e"), new Text("PRIVATE"));
            writer.append(new Text("f"), new Text("REDACT"));
        }

        // too lazy to create "correctly"
        final String gz = tempDir.toString() + ".gz";
        new Archiver().create("zip", new File(gz), tempDir.toFile());
        final String tgz = tempDir.toString() + ".tgz";
        new Archiver().create("tar", new File(tgz), tempDir.toFile());

        bloomMapFileResource = tgz;
    }

    @Test
    void testGetStatus() throws Exception {
        final BloomMapLookUpService lookup = new BloomMapLookUpService();
        lookup.initialize(new HashMap<>());
        lookup.loadResource(bloomMapFileResource);

        assertEquals(lookup.getStatus("a"), "PUBLIC");
        assertEquals(lookup.getStatus("b"), "PRIVATE");
        assertEquals(lookup.getStatus("c"), "REDACT");
        assertEquals(lookup.getStatus("d"), "PUBLIC");
        assertEquals(lookup.getStatus("e"), "PRIVATE");
        assertEquals(lookup.getStatus("f"), "REDACT");
        assertEquals(lookup.getStatus("g"), "DNE");
    }

    @Test
    void testGetFilter() throws Exception {
        final BloomMapLookUpService lookup = new BloomMapLookUpService();
        lookup.initialize(new HashMap<>());
        lookup.loadResource(bloomMapFileResource);

        final byte[] originalDynamicBloomFilter = IOUtils.readFullyToByteArray(
                new DataInputStream(
                        new FileInputStream(bloomMapFileResource.replace(".tgz", "/bloom"))
                ));
        lookup.getFilter("dynamic-hadoop-bloommap-2.10.0");

        final byte[] returnedDynamicBloomFilter = IOUtils.readFullyToByteArray(
                new DataInputStream(lookup.getFilter("dynamic-hadoop-bloommap-2.10.0")));

        assertArrayEquals(originalDynamicBloomFilter, returnedDynamicBloomFilter);
    }


}
