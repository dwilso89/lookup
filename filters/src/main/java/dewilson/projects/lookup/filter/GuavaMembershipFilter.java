package dewilson.projects.lookup.filter;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.stream.Stream;

public class GuavaMembershipFilter implements MembershipFilter {

    public static final String TYPE = "guava-29.0";
    private BloomFilter<String> filter;

    private GuavaMembershipFilter(final BloomFilter<String> bloomFilter) {
        this.filter = bloomFilter;
    }

    @Override
    public FilterResult test(final String member) {
        if (this.filter.mightContain(member)) {
            return FilterResult.MAY_EXIST;
        } else {
            return FilterResult.DOES_NOT_EXIST;
        }
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
    public final GuavaMembershipFilter read(final InputStream is) throws IOException {
        return new GuavaMembershipFilter(BloomFilter.readFrom(is, Funnels.stringFunnel(Charsets.UTF_8)));
    }

    public static class Builder implements MembershipFilterBuilder<MembershipFilter> {

        private double errorRate = 0.005F;
        private long expectedElements = 1000000;
        private Stream<String> elements;
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
        public GuavaMembershipFilter build() {
            if (!Strings.isNullOrEmpty(this.resource)) {
                try {
                    return new GuavaMembershipFilter(null)
                            .read(new BufferedInputStream(new FileInputStream(new File(this.resource))));
                } catch (final IOException ioe) {
                    throw new RuntimeException(String.format("Unable to read guava bloom filter from [%s]", this.resource));
                }
            } else {
                final BloomFilter<String> filter = filter();
                this.elements.forEach(filter::put);

                return new GuavaMembershipFilter(filter);
            }
        }

        @Override
        public Builder elements(final Stream<String> elements) {
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

        private BloomFilter<String> filter() {
            return BloomFilter.create(
                    Funnels.stringFunnel(Charsets.UTF_8),
                    this.expectedElements,
                    this.errorRate);
        }

    }
}
