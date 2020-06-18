package dewilson.projects.lookup.filter;

import bloomfilter.CanGenerateHashFrom;
import bloomfilter.mutable.BloomFilter;
import com.google.common.base.Strings;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.stream.Stream;

public class ScalaMembershipFilter implements MembershipFilter {

    public static final String TYPE = "scala";
    private BloomFilter<String> filter;

    private ScalaMembershipFilter(final BloomFilter<String> bloomFilter) {
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
    public final String getType() {
        return TYPE;
    }

    @Override
    public final void write(final OutputStream os) {
        this.filter.writeTo(os);
    }

    @Override
    public final ScalaMembershipFilter read(final InputStream is) {
        this.filter = BloomFilter.readFrom(is, CanGenerateHashFrom.CanGenerateHashFromStringByteArray$.MODULE$);
        return this;
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
        public ScalaMembershipFilter build() {
            if (!Strings.isNullOrEmpty(this.resource)) {
                try {
                    return new ScalaMembershipFilter(null).read(
                            new BufferedInputStream(new FileInputStream(new File(this.resource))));
                } catch (final IOException ioe) {
                    throw new RuntimeException(String.format("Unable to read scala bloom filter from [%s]", this.resource));
                }
            } else {
                final BloomFilter<String> bf = BloomFilter.apply(
                        this.expectedElements,
                        this.errorRate,
                        CanGenerateHashFrom.CanGenerateHashFromStringByteArray$.MODULE$);
                this.elements.forEach(bf::add);
                return new ScalaMembershipFilter(bf);
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

    }
}
