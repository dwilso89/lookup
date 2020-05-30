package dewilson.projects.lookup.filter;

import static org.junit.jupiter.api.Assertions.assertTrue;

import bloomfilter.CanGenerateHashFrom;
import bloomfilter.mutable.BloomFilter;
import com.google.common.base.Charsets;
import dewilson.projects.lookup.filter.impl.ScalaApproximateMembershipFilter;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.LongStream;

class ScalaApproximateMembershipFilterTest {

    @Test
    void buildAndUseFilterTest() {
        // testing no exception is thrown and probably exists on every key
        final ScalaApproximateMembershipFilter filter = new ScalaApproximateMembershipFilter.Builder()
                .elements(Arrays.asList(
                        "a".getBytes(Charsets.UTF_8),
                        "b".getBytes(Charsets.UTF_8),
                        "c".getBytes(Charsets.UTF_8),
                        "d".getBytes(Charsets.UTF_8))
                        .stream())
                .build();
        assertTrue(filter.probablyExists("a".getBytes(Charsets.UTF_8)));
        assertTrue(filter.probablyExists("b".getBytes(Charsets.UTF_8)));
        assertTrue(filter.probablyExists("c".getBytes(Charsets.UTF_8)));
        assertTrue(filter.probablyExists("d".getBytes(Charsets.UTF_8)));
    }

    @Disabled
    @Test
    void generateGiantBloomFilter() throws IOException {
        final bloomfilter.mutable.BloomFilter bf = BloomFilter.apply(
                1000000000,
                0.05F,
                CanGenerateHashFrom.CanGenerateHashFromLong$.MODULE$);

        LongStream.range(0L, 1000000000L).forEach(bf::add);

        try (final FileOutputStream fos = new FileOutputStream(new File("testFile.bf"))) {
            bf.writeTo(fos);
        }
    }

}
