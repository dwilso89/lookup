package dewilson.projects.lookup.filter;

import dewilso.projects.lookup.filter.ScalaBloomFilter;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

class ScalaBloomFilterTest {

    @Test
    void buildAndUseFilterTest() {
        // testing no exception is thrown and probably exists on every key
        final ScalaBloomFilter filter = new ScalaBloomFilter.Builder()
                .elements(Arrays.asList("a", "b", "c", "d").stream())
                .build();
        assertTrue(filter.probablyExists("a"));
        assertTrue(filter.probablyExists("b"));
        assertTrue(filter.probablyExists("c"));
        assertTrue(filter.probablyExists("d"));
    }

}
