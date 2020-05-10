package dewilson.projects.lookup.service;

import com.google.common.collect.Maps;
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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class BloomMapFileTest {

    private static String bloomMapFileResource;

    @BeforeAll
    static void createBloomMapFileArchive(@TempDir Path tempDir) throws Exception {
        final org.apache.hadoop.fs.Path bloomPath = new org.apache.hadoop.fs.Path(tempDir.toString() + "/bloomSmall");

        try (final BloomMapFile.Writer writer = new BloomMapFile.Writer(new Configuration(), bloomPath, BloomMapFile.Writer.keyClass(Text.class), BloomMapFile.Writer.valueClass(Text.class))) {
            writer.append(new Text("a"), new Text("PUBLIC"));
            writer.append(new Text("b"), new Text("PRIVATE"));
            writer.append(new Text("c"), new Text("REDACT"));
            writer.append(new Text("d"), new Text("PUBLIC"));
            writer.append(new Text("e"), new Text("PRIVATE"));
            writer.append(new Text("f"), new Text("REDACT"));
        }
/*
        // too lazy to create "correctly"
        final String gz = bloomOutput + ".gz";
        new Archiver().create("zip", new File(gz), new File(bloomOutput));
        final String tgz = bloomOutput + ".tgz";
        new Archiver().create("tar", new File(tgz), new File(bloomOutput));
        for(File file : tempDir.toFile().listFiles()){
            System.out.println(file.toString());
        }

        bloomMapFileResource = tgz;
        */

        for (File file : tempDir.toFile().listFiles()) {
            System.out.println(file.toString());
        }
        bloomMapFileResource = bloomPath.toString();
    }

    @Test
    void testExists(@TempDir Path tempDir) throws Exception {
        final BloomMapLookUpService lookup = new BloomMapLookUpService();
        Map<String, String> map = Maps.newHashMap();
        map.put("lookup.key.col", "0");
        map.put("lookup.val.col", "4");
        map.put("lookup.work.dir", tempDir.toString() + "/bloomBig");
        lookup.initialize(map);
        lookup.loadResource("src/test/resources/GOOG.csv");

        assertTrue(lookup.idExists("2020-05-05"));
    }

    @Test
    void testReadfromTgz() {

    }

    @Test
    void testGetStatus() throws Exception {
        final BloomMapLookUpService lookup = new BloomMapLookUpService();
        lookup.initialize(Maps.newHashMap());
        lookup.loadResource(bloomMapFileResource);

        assertEquals(lookup.getValue("a"), "PUBLIC");
        assertEquals(lookup.getValue("b"), "PRIVATE");
        assertEquals(lookup.getValue("c"), "REDACT");
        assertEquals(lookup.getValue("d"), "PUBLIC");
        assertEquals(lookup.getValue("e"), "PRIVATE");
        assertEquals(lookup.getValue("f"), "REDACT");
        assertEquals(lookup.getValue("g"), "DNE");
    }

    @Test
    void testGetFilter() throws Exception {
        final BloomMapLookUpService lookup = new BloomMapLookUpService();
        lookup.initialize(Maps.newHashMap());
        lookup.loadResource(bloomMapFileResource);

        final byte[] originalDynamicBloomFilter = IOUtils.readFullyToByteArray(
                new DataInputStream(new FileInputStream(bloomMapFileResource + "/bloom")));
        lookup.getFilter("dynamic-hadoop-bloommap-2.10.0");

        final byte[] returnedDynamicBloomFilter = IOUtils.readFullyToByteArray(
                new DataInputStream(lookup.getFilter("dynamic-hadoop-bloommap-2.10.0")));

        assertArrayEquals(originalDynamicBloomFilter, returnedDynamicBloomFilter);
    }


}
