package dewilson.projects.lookup.reader;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

public interface KVReader<K, V> {

    String getType();

    KVReader initialize(final String resource, final Map<String, String> configuration);

    Stream<Pair> getKVStream() throws IOException;

    class Pair {

        private final String key;
        private final String value;

        public Pair(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return this.key;
        }

        public String getValue() {
            return this.value;
        }
    }


}
