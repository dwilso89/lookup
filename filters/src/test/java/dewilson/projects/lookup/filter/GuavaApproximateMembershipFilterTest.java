package dewilson.projects.lookup.filter;

import com.google.common.base.Charsets;
import com.google.common.hash.Funnels;
import com.google.common.primitives.Longs;
import dewilson.projects.lookup.filter.impl.GuavaApproximateMembershipFilter;
import dewilson.projects.lookup.filter.impl.ScalaApproximateMembershipFilter;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

class GuavaApproximateMembershipFilterTest {

    @Test
    void buildAndUseFilterTest() {
        // testing no exception is thrown and probably exists on every key
        final GuavaApproximateMembershipFilter filter = new GuavaApproximateMembershipFilter.Builder()
                .elements(Arrays.asList
                        ("a".getBytes(Charsets.UTF_8),
                                "b".getBytes(Charsets.UTF_8),
                                "c".getBytes(Charsets.UTF_8),
                                "d".getBytes(Charsets.UTF_8)
                        ).stream())
                .build();
        assertTrue(filter.probablyExists("a".getBytes(Charsets.UTF_8)));
        assertTrue(filter.probablyExists("b".getBytes(Charsets.UTF_8)));
        assertTrue(filter.probablyExists("c".getBytes(Charsets.UTF_8)));
        assertTrue(filter.probablyExists("d".getBytes(Charsets.UTF_8)));
    }

    @Disabled
    @Test
    void generateGiantBloomFilter() throws IOException {
        final GuavaApproximateMembershipFilter bf = new GuavaApproximateMembershipFilter.Builder()
                .expectedElements(1000000000L)
                .elements(LongStream.range(0L, 1000000000L).mapToObj(Longs::toByteArray))
                .build();

        // TODO make windows friendly tmp location
        final File outputFile = new File("/tmp/filters/testGuavaFile.apmf");
        outputFile.getParentFile().mkdir();
        try (final FileOutputStream fos = new FileOutputStream(outputFile)) {
            bf.write(fos);
        }

    }


}
