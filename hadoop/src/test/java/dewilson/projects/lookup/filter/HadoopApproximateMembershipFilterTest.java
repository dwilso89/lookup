package dewilson.projects.lookup.filter;

import com.google.common.base.Charsets;
import com.google.common.primitives.Longs;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

class HadoopApproximateMembershipFilterTest {

    @Test
    void buildAndUseFilterTest() {
        // testing no exception is thrown and probably exists on every key
        final HadoopApproximateMembershipFilter filter = new HadoopApproximateMembershipFilter.Builder()
                .elements(Stream.of(
                        "a".getBytes(Charsets.UTF_8),
                        "b".getBytes(Charsets.UTF_8),
                        "c".getBytes(Charsets.UTF_8),
                        "d".getBytes(Charsets.UTF_8)))
                .build();
        assertTrue(filter.probablyExists("a".getBytes(Charsets.UTF_8)));
        assertTrue(filter.probablyExists("b".getBytes(Charsets.UTF_8)));
        assertTrue(filter.probablyExists("c".getBytes(Charsets.UTF_8)));
        assertTrue(filter.probablyExists("d".getBytes(Charsets.UTF_8)));
    }

    @Disabled
    @Test
    void generateGiantBloomFilter() throws IOException {
        final HadoopApproximateMembershipFilter bf = new HadoopApproximateMembershipFilter.Builder()
                .elements(LongStream.range(0L, 1000000000L).mapToObj(Longs::toByteArray))
                .build();

        // TODO make windows friendly tmp location
        final File outputFile = new File("/tmp/filters/testHadoopFile.apmf");
        outputFile.getParentFile().mkdir();
        try (final FileOutputStream fos = new FileOutputStream(outputFile)) {
            bf.write(fos);
        }
    }

}
