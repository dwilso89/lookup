package dewilson.projects.lookup.filter.impl;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import dewilson.projects.lookup.filter.api.ApproximateMembershipFilter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.stream.Stream;

public class GuavaApproximateMembershipFilter implements ApproximateMembershipFilter {

    private final BloomFilter<byte[]> filter;

    private GuavaApproximateMembershipFilter(final BloomFilter<byte[]> bloomFilter) {
        this.filter = bloomFilter;
    }

    @Override
    public final boolean probablyExists(final byte[] key) {
        return this.filter.mightContain(key);
    }

    @Override
    public final void write(final OutputStream os) throws IOException {
        this.filter.writeTo(os);
    }

    @Override
    public final String getType() {
        return "guava";
    }

    @Override
    public final GuavaApproximateMembershipFilter read(final InputStream is) throws IOException {
        return new GuavaApproximateMembershipFilter(BloomFilter.readFrom(is, Funnels.byteArrayFunnel()));
    }

    public static class Builder {

        private double errorRate = 0.005F;
        private long expectedElements = 1000000;
        private Stream<byte[]> elements;

        public GuavaApproximateMembershipFilter build() {
            final BloomFilter<byte[]> filter = BloomFilter.create(
                    Funnels.byteArrayFunnel(),
                    this.expectedElements,
                    this.errorRate);

            this.elements.forEach(filter::put);

            return new GuavaApproximateMembershipFilter(filter);
        }

        public Builder expectedElements(final long expectedElements) {
            this.expectedElements = expectedElements;
            return this;
        }

        public Builder errorRate(final double errorRate) {
            this.errorRate = errorRate;
            return this;
        }

        public Builder elements(final Stream<byte[]> elements) {
            this.elements = elements;
            return this;
        }

    }
}
