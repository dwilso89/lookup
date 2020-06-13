package dewilson.projects.lookup.filter.impl;

import com.google.common.base.Strings;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import dewilson.projects.lookup.filter.api.ApproximateMembershipFilter;
import dewilson.projects.lookup.filter.api.ApproximateMembershipFilterBuilder;

import java.io.*;
import java.util.Map;
import java.util.stream.Stream;

public class GuavaApproximateMembershipFilter implements ApproximateMembershipFilter {

    public static final String TYPE = "guava-29.0";

    private BloomFilter<byte[]> filter;

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
        return TYPE;
    }

    @Override
    public final GuavaApproximateMembershipFilter read(final InputStream is) throws IOException {
        return new GuavaApproximateMembershipFilter(BloomFilter.readFrom(is, Funnels.byteArrayFunnel()));
    }

    public static class Builder implements ApproximateMembershipFilterBuilder {

        private double errorRate = 0.005F;
        private long expectedElements = 1000000;
        private Stream<byte[]> elements;
        private String resource;

        public Builder() {
            // empty on purpose
        }

        @Override
        public String getType() {
            return TYPE;
        }

        @Override
        public Builder initialize(final Map<String, String> configuration) {
            // if points to serialized filter
            final String resource = configuration.get("lookUp.filter.guava.resource");
            if (!Strings.isNullOrEmpty(resource)) {
                this.resource = resource;
                return this;
            } else {
                final String expectedElementsConfig = configuration.get("lookUp.filter.guava.expectedElements");
                if (expectedElementsConfig != null) {
                    expectedElements(Long.valueOf(expectedElementsConfig));
                }
                final String errorRateConfig = configuration.get("lookUp.filter.guava.errorRate");
                if (errorRateConfig != null) {
                    errorRate(Double.valueOf(errorRateConfig));
                }

                return this;
            }
        }

        @Override
        public GuavaApproximateMembershipFilter build() {
            if (!Strings.isNullOrEmpty(this.resource)) {
                try {
                    return new GuavaApproximateMembershipFilter(null)
                            .read(new BufferedInputStream(new FileInputStream(new File(this.resource))));
                } catch (final IOException ioe) {
                    throw new RuntimeException(String.format("Unable to read guava bloom filter from [%s]", this.resource));
                }
            } else {
                final BloomFilter<byte[]> filter = filter();
                this.elements.forEach(filter::put);

                return new GuavaApproximateMembershipFilter(filter);
            }
        }

        @Override
        public Builder elements(final Stream<byte[]> elements) {
            this.elements = elements;
            return this;
        }

        public Builder expectedElements(final long expectedElements) {
            this.expectedElements = expectedElements;
            return this;
        }

        public Builder errorRate(final double errorRate) {
            this.errorRate = errorRate;
            return this;
        }

        private BloomFilter<byte[]> filter() {
            return BloomFilter.create(
                    Funnels.byteArrayFunnel(),
                    this.expectedElements,
                    this.errorRate);
        }

    }
}
