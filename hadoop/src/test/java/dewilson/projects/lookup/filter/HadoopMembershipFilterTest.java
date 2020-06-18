package dewilson.projects.lookup.filter;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HadoopMembershipFilterTest {

    @Test
    void buildAndUseFilterTest() {
        // testing no exception is thrown and probably exists on every key
        final HadoopMembershipFilter filter = new HadoopMembershipFilter.Builder()
                .elements(Stream.of("a", "b", "c", "d"))
                .build();

        assertEquals(FilterResult.MAY_EXIST, filter.test("a"));
        assertEquals(FilterResult.MAY_EXIST, filter.test("b"));
        assertEquals(FilterResult.MAY_EXIST, filter.test("c"));
        assertEquals(FilterResult.MAY_EXIST, filter.test("d"));
    }

    @Disabled
    @Test
    void generateGiantBloomFilter() throws IOException {
        final HadoopMembershipFilter bf = new HadoopMembershipFilter.Builder()
                .elements(LongStream.range(0L, 1000000000L).mapToObj(Long::toString))
                .build();

        // TODO make windows friendly tmp location
        final File outputFile = new File("/tmp/filters/testHadoopFile.apmf");
        assertTrue(outputFile.getParentFile().mkdir());
        try (final FileOutputStream fos = new FileOutputStream(outputFile)) {
            bf.write(fos);
        }
    }

}
