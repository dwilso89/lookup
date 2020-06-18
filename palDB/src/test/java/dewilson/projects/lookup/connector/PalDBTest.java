package dewilson.projects.lookup.connector;

import com.google.common.collect.Maps;
import com.linkedin.paldb.api.PalDB;
import com.linkedin.paldb.api.StoreWriter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PalDBTest {

    private static String palDBResource;

    @BeforeAll
    static void create(@TempDir Path tempDir) {
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
        final PalDBLookUpConnector lookup = new PalDBLookUpConnector();
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
        final LookUpConnector lookup = new PalDBLookUpConnector();
        final Map<String, String> map = Maps.newHashMap();
        map.put("lookUp.key.col", "0");
        map.put("lookUp.val.col", "4");
        map.put("lookUp.work.dir", tempDir.toString() + "/csv");
        map.put("lookUp.connector.resource.type", "csv");
        lookup.initialize(map);
        lookup.loadResource("src/test/resources/GOOG_2020.csv");

        assertTrue(lookup.keyExists("2020-05-05"));
        assertFalse(lookup.keyExists("2025-05-05"));
    }

}
