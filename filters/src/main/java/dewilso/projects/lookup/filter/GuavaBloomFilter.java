package dewilso.projects.lookup.filter;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.stream.Stream;

public class GuavaBloomFilter implements dewilso.projects.lookup.filter.BloomFilter<String> {

    private final BloomFilter<String> filter;

    private GuavaBloomFilter(final BloomFilter<String> bloomFilter) {
        this.filter = bloomFilter;
    }

    @Override
    public boolean probablyExists(final String key) {
        return this.filter.mightContain(key);
    }

    @Override
    public void write(final OutputStream os) throws IOException {
        this.filter.writeTo(os);
    }

    @Override
    public GuavaBloomFilter read(final InputStream is) throws IOException {
        return new GuavaBloomFilter(BloomFilter.readFrom(is, Funnels.stringFunnel(Charsets.UTF_8)));
    }

    public static class Builder {

        private double errorRate = .01;
        private long expectedElements = 1000000;
        private Stream<Object> elements;

        public GuavaBloomFilter build() {
            final BloomFilter<String> filter = BloomFilter.create(
                    Funnels.stringFunnel(Charsets.UTF_8),
                    this.expectedElements,
                    this.errorRate);

            this.elements.forEach(obj -> filter.put(obj.toString()));

            return new GuavaBloomFilter(filter);
        }

        public Builder expectedElements(final long expectedElements) {
            this.expectedElements = expectedElements;
            return this;
        }

        public Builder errorRate(final double errorRate) {
            this.errorRate = errorRate;
            return this;
        }

        public Builder elements(final Stream<Object> elements) {
            this.elements = elements;
            return this;
        }

    }
}
