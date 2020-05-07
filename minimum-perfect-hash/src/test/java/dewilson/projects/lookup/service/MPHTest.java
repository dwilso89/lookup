package dewilson.projects.lookup.service;

import com.indeed.mph.TableConfig;
import com.indeed.mph.TableWriter;
import com.indeed.mph.serializers.SmartStringSerializer;
import com.indeed.util.core.Pair;
import dewilson.projects.lookup.support.SimpleSupport;
import dewilson.projects.lookup.support.Support;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class MPHTest {

    private static String mphResource;

    @BeforeAll
    public static void createBloomMapFileArchive(@TempDir Path tempDir) throws Exception {
        final org.apache.hadoop.fs.Path bloomPath = new org.apache.hadoop.fs.Path(tempDir.toString());
        final TableConfig<String, String> config = new TableConfig()
                .withKeySerializer(new SmartStringSerializer())
                .withValueSerializer(new SmartStringSerializer());

        final Set<Pair<String, String>> entries = new HashSet<>();
        entries.add(new Pair<>("a", "PUBLIC"));
        entries.add(new Pair<>("b", "PRIVATE"));
        entries.add(new Pair<>("c", "REDACT"));
        entries.add(new Pair<>("d", "PUBLIC"));
        entries.add(new Pair<>("e", "PRIVATE"));
        entries.add(new Pair<>("f", "REDACT"));
        TableWriter.write(tempDir.toFile(), config, entries);

        mphResource = tempDir.toString();
    }

    @Test
    void testGetStatus() throws Exception {
        final MPHLookUpService lookup = new MPHLookUpService();
        lookup.initialize(new HashMap<>());
        lookup.loadResource(mphResource);

        assertEquals(lookup.getStatus("a"), "PUBLIC");
        assertEquals(lookup.getStatus("b"), "PRIVATE");
        assertEquals(lookup.getStatus("c"), "REDACT");
        assertEquals(lookup.getStatus("d"), "PUBLIC");
        assertEquals(lookup.getStatus("e"), "PRIVATE");
        assertEquals(lookup.getStatus("f"), "REDACT");
        assertEquals(lookup.getStatus("g"), "DNE");
    }

    @Test
    void testGetFilter() {
        // TODO throws exception
    }

    @Test
    void testGetSupportedStatuses() throws Exception {
        final MPHLookUpService lookup = new MPHLookUpService();
        lookup.initialize(new HashMap<>());
        lookup.loadResource(mphResource);

        final Support statusSupport = new SimpleSupport();
        statusSupport.addSupportedType("DNE");

        assertNotEquals(statusSupport, lookup.getStatusSupport());

        statusSupport.addSupportedType("PUBLIC");
        statusSupport.addSupportedType("PRIVATE");
        statusSupport.addSupportedType("REDACT");

        assertEquals(statusSupport, lookup.getStatusSupport());
    }


}
