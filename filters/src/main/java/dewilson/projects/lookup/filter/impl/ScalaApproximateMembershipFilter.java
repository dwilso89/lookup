package dewilson.projects.lookup.filter.impl;

import bloomfilter.CanGenerateHashFrom;
import bloomfilter.mutable.BloomFilter;
import com.google.common.base.Strings;
import dewilson.projects.lookup.filter.api.ApproximateMembershipFilter;
import dewilson.projects.lookup.filter.api.ApproximateMembershipFilterBuilder;

import java.io.*;
import java.util.Map;
import java.util.stream.Stream;

public class ScalaApproximateMembershipFilter implements ApproximateMembershipFilter {

    public static final String TYPE = "scala";
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
        return TYPE;
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
            final String resource = configuration.get("lookUp.filter.scala.resource");
            if (!Strings.isNullOrEmpty(resource)) {
                this.resource = resource;
                return this;
            } else {
                final String expectedElementsConfig = configuration.get("filter.scala.expectedElements");
                if (expectedElementsConfig != null) {
                    expectedElements(Long.valueOf(expectedElementsConfig));
                }
                final String errorRateConfig = configuration.get("filter.scala.errorRate");
                if (errorRateConfig != null) {
                    errorRate(Double.valueOf(errorRateConfig));
                }

                return this;
            }
        }

        @Override
        public ScalaApproximateMembershipFilter build() {
            if (!Strings.isNullOrEmpty(this.resource)) {
                try {
                    return new ScalaApproximateMembershipFilter(null).read(
                            new BufferedInputStream(new FileInputStream(new File(this.resource))));
                } catch (final IOException ioe) {
                    throw new RuntimeException(String.format("Unable to read scala bloom filter from [%s]", this.resource));
                }
            } else {
                final BloomFilter<byte[]> bf = BloomFilter.apply(
                        this.expectedElements,
                        this.errorRate,
                        CanGenerateHashFrom.CanGenerateHashFromByteArray$.MODULE$);
                this.elements.forEach(bf::add);
                return new ScalaApproximateMembershipFilter(bf);
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

    }
}
