package dewilson.projects.lookup.filter.impl;

import bloomfilter.CanGenerateHashFrom;
import bloomfilter.mutable.BloomFilter;
import dewilson.projects.lookup.filter.api.ApproximateMembershipFilter;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.stream.Stream;

public class ScalaApproximateMembershipFilter implements ApproximateMembershipFilter {

    private BloomFilter<byte[]> filter;

    private ScalaApproximateMembershipFilter(final BloomFilter<byte[]> bloomFilter) {
        this.filter = bloomFilter;
    }

    @Override
    public final boolean probablyExists(final byte[] key) {
        return this.filter.mightContain(key);
    }

    @Override
    public final String getType() {
        return "scala";
    }

    @Override
    public final void write(final OutputStream os) {
        this.filter.writeTo(os);
    }

    @Override
    public final ScalaApproximateMembershipFilter read(final InputStream is) {
        this.filter = BloomFilter.readFrom(is, CanGenerateHashFrom.CanGenerateHashFromByteArray$.MODULE$);
        return this;
    }

    public static class Builder {

        private double errorRate = .01;
        private long expectedElements = 1000000;
        private Stream<byte[]> elements;

        public ScalaApproximateMembershipFilter build() {
            final BloomFilter<byte[]> bf = BloomFilter.apply(
                    this.expectedElements,
                    this.errorRate,
                    CanGenerateHashFrom.CanGenerateHashFromByteArray$.MODULE$);
            this.elements.forEach(bf::add);
            return new ScalaApproximateMembershipFilter(bf);
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
