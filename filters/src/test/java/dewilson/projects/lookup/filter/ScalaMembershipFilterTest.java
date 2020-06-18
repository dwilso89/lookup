package dewilson.projects.lookup.filter;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScalaMembershipFilterTest {

    @Test
    void buildAndUseFilterTest() {
        // testing no exception is thrown and probably exists on every key
        final ScalaMembershipFilter filter = new ScalaMembershipFilter.Builder()
                .elements(Stream.of("a", "b", "c", "d"))
                .build();
        assertEquals(FilterResult.MAY_EXIST, filter.test("a"));
        assertEquals(FilterResult.MAY_EXIST, filter.test("b"));
        assertEquals(FilterResult.MAY_EXIST, filter.test("c"));
        assertEquals(FilterResult.MAY_EXIST, filter.test("d"));
    }

    @Test
    void buildFilterWithFactory() {
        final Stream<String> keyStream = Stream.of("a", "b", "c", "d");
        final Map<String, String> configuration = Maps.newHashMap();
        configuration.put("lookUp.filter.type", ScalaMembershipFilter.TYPE);
        final MembershipFilter filter = FilterFactory.getMembershipFilter(
                ScalaMembershipFilter.TYPE, configuration, keyStream);

        assertEquals(FilterResult.MAY_EXIST, filter.test("a"));
        assertEquals(FilterResult.MAY_EXIST, filter.test("b"));
        assertEquals(FilterResult.MAY_EXIST, filter.test("c"));
        assertEquals(FilterResult.MAY_EXIST, filter.test("d"));
    }

    @Disabled
    @Test
    void generateGiantBloomFilter() throws IOException {
        final ScalaMembershipFilter bf = new ScalaMembershipFilter.Builder()
                .expectedElements(1000000000L)
                .elements(LongStream.range(0L, 1000000000L).mapToObj(String::valueOf))
                .build();

        // TODO make windows friendly tmp location
        final File outputFile = new File("/tmp/filters/testScalaFile.apmf");
        assertTrue(outputFile.getParentFile().mkdir());
        try (final FileOutputStream fos = new FileOutputStream(outputFile)) {
            bf.write(fos);
        }

    }

}
