package dewilso.projects.lookup.filter;

import bloomfilter.CanGenerateHashFrom;
import bloomfilter.mutable.BloomFilter;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.stream.Stream;

public class ScalaBloomFilter implements dewilso.projects.lookup.filter.BloomFilter<String> {

    private BloomFilter<String> filter;

    private ScalaBloomFilter(final BloomFilter<String> bloomFilter) {
        this.filter = bloomFilter;
    }

    @Override
    public boolean probablyExists(final String key) {
        return this.filter.mightContain(key);
    }

    @Override
    public void write(final OutputStream os) {
        this.filter.writeTo(os);
    }

    @Override
    public ScalaBloomFilter read(final InputStream is) {
        this.filter = BloomFilter.readFrom(is, CanGenerateHashFrom.CanGenerateHashFromString$.MODULE$);
        return this;
    }

    public static class Builder {

        private double errorRate = .01;
        private long expectedElements = 1000000;
        private Stream<String> elements;

        public ScalaBloomFilter build() {
            final BloomFilter<String> bf = BloomFilter.apply(
                    this.expectedElements,
                    this.errorRate,
                    CanGenerateHashFrom.CanGenerateHashFromStringByteArray$.MODULE$);
            this.elements.forEach(bf::add);
            return new ScalaBloomFilter(bf);
        }

        public Builder expectedElements(final long expectedElements) {
            this.expectedElements = expectedElements;
            return this;
        }

        public Builder errorRate(final double errorRate) {
            this.errorRate = errorRate;
            return this;
        }

        public Builder elements(final Stream<String> elements) {
            this.elements = elements;
            return this;
        }

    }
}
