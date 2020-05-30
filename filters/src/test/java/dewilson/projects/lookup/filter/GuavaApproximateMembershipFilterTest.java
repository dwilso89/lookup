package dewilson.projects.lookup.filter;

import com.google.common.base.Charsets;
import com.google.common.hash.Funnels;
import dewilson.projects.lookup.filter.impl.GuavaApproximateMembershipFilter;
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
        final com.google.common.hash.BloomFilter<Long> filter = com.google.common.hash.BloomFilter.create(
                Funnels.longFunnel(),
                1000000000,
                0.05F);

        LongStream.range(0, 1000000000L).forEach(filter::put);

        try (final FileOutputStream fos = new FileOutputStream(new File("testGuavaFile.bf"))) {
            filter.writeTo(fos);
        }
    }


}
