package dewilson.projects.lookup.connector;

import com.google.common.collect.Maps;
import dewilson.projects.lookup.api.connector.LookUpConnector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
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

        bloomMapFileResource = bloomPath.toString();
    }

    @Test
    void testExists(@TempDir Path tempDir) throws Exception {
        final LookUpConnector lookup = new BloomMapLookUpConnector();
        final Map<String, String> map = Maps.newHashMap();
        map.put("lookUp.key.col", "0");
        map.put("lookUp.val.col", "4");
        map.put("lookUp.work.dir", tempDir.toString() + "/bloomBig");
        map.put("lookUp.connector.resource.type", "csv");
        lookup.initialize(map);
        lookup.loadResource("src/test/resources/GOOG_2020.csv");

        long start = System.currentTimeMillis();
        for(int i = 0; i < 100000; i++){
            lookup.idExists("2025-05-05");
        }
        System.out.println(System.currentTimeMillis() - start);

        assertTrue(lookup.idExists("2020-05-05"));
        assertFalse(lookup.idExists("2025-05-05"));
    }

    @Test
    void testGetValue() throws Exception {
        final BloomMapLookUpConnector lookup = new BloomMapLookUpConnector();
        lookup.initialize(Map.of("lookUp.connector.resource.type", "dir"));
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
        final BloomMapLookUpConnector lookup = new BloomMapLookUpConnector();
        lookup.initialize(Map.of("lookUp.connector.resource.type", "dir"));
        lookup.loadResource(bloomMapFileResource);

        final byte[] originalDynamicBloomFilter = IOUtils.readFullyToByteArray(
                new DataInputStream(new FileInputStream(bloomMapFileResource + "/bloom")));
        lookup.getFilter("dynamic-hadoop-bloommap-2.10.0");

        final byte[] returnedDynamicBloomFilter = IOUtils.readFullyToByteArray(
                new DataInputStream(lookup.getFilter("dynamic-hadoop-bloommap-2.10.0")));

        assertArrayEquals(originalDynamicBloomFilter, returnedDynamicBloomFilter);
    }


}
