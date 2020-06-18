package dewilson.projects.lookup.filter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class KeySetFilter implements MembershipFilter {

    private static final String TYPE = "keyset";
    private final Set<String> keySet;

    private KeySetFilter() {
        this.keySet = new HashSet<>();
    }

    private void addKeyToFilter(final String key) {
        this.keySet.add(key);
    }

    @Override
    public FilterResult test(final String key) {
        return this.keySet.contains(key) ? FilterResult.EXISTS : FilterResult.DOES_NOT_EXIST;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void write(final OutputStream os) throws IOException {
        final DataOutputStream dos = new DataOutputStream(new GZIPOutputStream(os));
        for (final String key : this.keySet) {
            dos.writeUTF(key);
        }
    }

    @Override
    public KeySetFilter read(final InputStream is) throws IOException {
        final DataInputStream dis = new DataInputStream(new GZIPInputStream(is));
        while (dis.available() > 0) {
            this.keySet.add(dis.readUTF());
        }
        return this;
    }

    public static class Builder implements MembershipFilterBuilder<KeySetFilter> {

        private Stream<String> elements;

        @Override
        public String getType() {
            return TYPE;
        }

        @Override
        public MembershipFilterBuilder<KeySetFilter> initialize(final Map<String, String> configuration) {
            return this;
        }

        @Override
        public MembershipFilterBuilder<KeySetFilter> elements(final Stream<String> elements) {
            this.elements = elements;
            return this;
        }

        @Override
        public KeySetFilter build() {
            final KeySetFilter keySetFilter = new KeySetFilter();
            this.elements.forEach(keySetFilter::addKeyToFilter);
            return keySetFilter;
        }
    }
}
