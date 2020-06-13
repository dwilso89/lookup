package dewilson.projects.lookup.filter.impl;

import dewilson.projects.lookup.filter.api.ApproximateMembershipFilter;
import dewilson.projects.lookup.filter.api.Filter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.stream.Stream;

public class XorApproximateMembershipFilter implements ApproximateMembershipFilter {

    // TODO find __published java xorfilter implementation

    /*
    private final XorFilter xorFilter;

    private XorApproximateMembershipFilter(final XorFilter xorFilter){
        this.xorFilter=xorFilter;
    }
    */

    @Override
    public boolean probablyExists(final byte[] member) {
        return false;
    }

    @Override
    public String getType() {
        return null;
    }

    @Override
    public void write(final OutputStream os) throws IOException {
    }

    @Override
    public XorApproximateMembershipFilter read(final InputStream is) throws IOException {
        return null;
    }

    public static class Builder {

        private double errorRate = 0.005F;
        private long expectedElements = 1000000;
        private Stream<byte[]> elements;

        public XorApproximateMembershipFilter build() {

         /*
            final XorFilter<byte[]> filter = ...;
            this.elements.forEach(filter::put);
            return new XorApproximateMembershipFilter(filter);
            */

            return null;
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
