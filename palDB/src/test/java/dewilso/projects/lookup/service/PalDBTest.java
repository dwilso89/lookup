package dewilso.projects.lookup.service;

import com.google.common.collect.Maps;
import com.linkedin.paldb.api.PalDB;
import com.linkedin.paldb.api.StoreWriter;
import dewilson.projects.lookup.service.LookUpService;
import dewilson.projects.lookup.service.PalDBLookUpService;
import dewilson.projects.lookup.support.DefaultSupportTypes;
import dewilson.projects.lookup.support.SimpleSupport;
import dewilson.projects.lookup.support.Support;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class PalDBTest {

    private static String palDBResource;

    @BeforeAll
    static void create(@TempDir Path tempDir) throws Exception {
        palDBResource = tempDir.toString() + "/palDB-test";
        final StoreWriter writer = PalDB.createWriter(new File(palDBResource));
        writer.put("a", "PUBLIC");
        writer.put("b", "PRIVATE");
        writer.put("c", "REDACT");
        writer.put("d", "PUBLIC");
        writer.put("e", "PRIVATE");
        writer.put("f", "REDACT");
        writer.close();
    }

    @Test
    void testGetValue() throws Exception {
        final PalDBLookUpService lookup = new PalDBLookUpService();
        lookup.initialize(new HashMap<>());
        lookup.loadResource(palDBResource);

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
        final LookUpService lookup = new PalDBLookUpService();
        final Map<String, String> map = Maps.newHashMap();
        map.put("lookUp.key.col", "0");
        map.put("lookUp.val.col", "4");
        map.put("lookUp.work.dir", tempDir.toString() + "/csv");
        map.put("lookUp.resourceType", "csv");
        lookup.initialize(map);
        lookup.loadResource("src/test/resources/GOOG_2020.csv");

        assertTrue(lookup.idExists("2020-05-05"));
        assertFalse(lookup.idExists("2025-05-05"));
    }

    @Test
    void testGetValues() throws Exception {
        final LookUpService lookup = new PalDBLookUpService();
        lookup.initialize(new HashMap<>());
        lookup.loadResource(palDBResource);

        final Support valueSupport = new SimpleSupport(DefaultSupportTypes.VALUE);
        valueSupport.addSupport("DNE");

        assertNotEquals(valueSupport, lookup.getValueSupport());

        valueSupport.addSupport("PUBLIC");
        valueSupport.addSupport("PRIVATE");
        valueSupport.addSupport("REDACT");

        assertEquals(valueSupport, lookup.getValueSupport());
    }


}
