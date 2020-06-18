package dewilson.projects.lookup.filter;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.fastfilter.Filter;
import org.fastfilter.bloom.Bloom;
import org.fastfilter.cuckoo.Cuckoo16;
import org.fastfilter.cuckoo.Cuckoo8;
import org.fastfilter.xor.Xor16;
import org.fastfilter.xor.Xor8;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.stream.Stream;

public class FastFilterMembershipFilter implements MembershipFilter {

    private final Filter filter;
    private final String type;

    private FastFilterMembershipFilter(final Filter filter, final String type) {
        this.filter = filter;
        this.type = type;
    }

    @Override
    public FilterResult test(final String member) {
        if (this.filter.mayContain(hash(member))) {
            return FilterResult.MAY_EXIST;
        } else {
            return FilterResult.DOES_NOT_EXIST;
        }
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public void write(final OutputStream os) throws IOException {
        // TODO serialize FastFilters
    }

    @Override
    public FastFilterMembershipFilter read(final InputStream is) throws IOException {
        // See write comment
        return null;
    }

    private static long hash(final String data) {
        return Hashing.murmur3_128().hashString(data, Charsets.UTF_8).asLong();
    }

    public static class Builder implements MembershipFilterBuilder<FastFilterMembershipFilter> {

        private static final String TYPE = "fastfilter";
        private String filterType = "Xor8";
        private Stream<String> elements;

        @Override
        public Builder initialize(final Map<String, String> configuration) {
            this.filterType = configuration.getOrDefault("lookUp.filter.fastfilter.type", this.filterType);
            return this;
        }

        @Override
        public String getType() {
            return TYPE;
        }

        @Override
        public FastFilterMembershipFilter build() {
            final long[] keys = this.elements
                    .map(element -> Hashing.murmur3_128().hashString(element, Charsets.UTF_8))
                    .mapToLong(HashCode::asLong)
                    .toArray();

            switch (this.filterType) {
                case "Xor8":
                    return new FastFilterMembershipFilter(Xor8.construct(keys), this.filterType);
                case "Xor16":
                    return new FastFilterMembershipFilter(Xor16.construct(keys), this.filterType);
                case "bloom":
                    return new FastFilterMembershipFilter(Bloom.construct(keys, 64), this.filterType);
                case "cuckoo8":
                    return new FastFilterMembershipFilter(Cuckoo8.construct(keys), this.filterType);
                case "cuckoo16":
                    return new FastFilterMembershipFilter(Cuckoo16.construct(keys), this.filterType);
                default:
                    throw new UnsupportedOperationException(
                            String.format("FastFilter with type [%s] is not supported.", this.filterType));
            }
        }

        @Override
        public Builder elements(final Stream<String> elements) {
            this.elements = elements;
            return this;
        }

    }

}
