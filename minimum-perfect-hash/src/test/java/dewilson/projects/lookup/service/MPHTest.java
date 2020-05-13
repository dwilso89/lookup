package dewilson.projects.lookup.service;

import com.google.common.collect.Maps;
import com.indeed.mph.TableConfig;
import com.indeed.mph.TableWriter;
import com.indeed.mph.serializers.SmartStringSerializer;
import com.indeed.util.core.Pair;
import dewilson.projects.lookup.support.DefaultSupportTypes;
import dewilson.projects.lookup.support.SimpleSupport;
import dewilson.projects.lookup.support.Support;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class MPHTest {

    private static String mphResource;

    @BeforeAll
    static void create(@TempDir Path tempDir) throws Exception {
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

        assertEquals(lookup.getValue("a"), "PUBLIC");
        assertEquals(lookup.getValue("b"), "PRIVATE");
        assertEquals(lookup.getValue("c"), "REDACT");
        assertEquals(lookup.getValue("d"), "PUBLIC");
        assertEquals(lookup.getValue("e"), "PRIVATE");
        assertEquals(lookup.getValue("f"), "REDACT");
        assertEquals(lookup.getValue("g"), "DNE");
    }

    @Test
    void testExists(@TempDir Path tempDir) throws Exception {
        final LookUpService lookup = new MPHLookUpService();
        final Map<String, String> map = Maps.newHashMap();
        map.put("lookUp.key.col", "0");
        map.put("lookUp.val.col", "4");
        map.put("lookUp.work.dir", tempDir.toString() + "/palDB");
        map.put("lookUp.resourceType", "csv");
        lookup.initialize(map);
        lookup.loadResource("src/test/resources/GOOG_2020.csv");

        assertTrue(lookup.idExists("2020-05-05"));
        assertFalse(lookup.idExists("2025-05-05"));
    }

    @Test
    void testGetValues() throws Exception {
        final MPHLookUpService lookup = new MPHLookUpService();
        lookup.initialize(new HashMap<>());
        lookup.loadResource(mphResource);

        final Support valueSupport = new SimpleSupport(DefaultSupportTypes.VALUE);
        valueSupport.addSupport("DNE");

        assertNotEquals(valueSupport, lookup.getValueSupport());

        valueSupport.addSupport("PUBLIC");
        valueSupport.addSupport("PRIVATE");
        valueSupport.addSupport("REDACT");

        assertEquals(valueSupport, lookup.getValueSupport());
    }


}
