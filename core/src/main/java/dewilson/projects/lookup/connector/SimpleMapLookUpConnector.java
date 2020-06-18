package dewilson.projects.lookup.connector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class SimpleMapLookUpConnector implements LookUpConnector {

    private final Map<String, String> dataMap;

    public SimpleMapLookUpConnector() {
        this.dataMap = new ConcurrentHashMap<>();
    }

    @Override
    public void initialize(final Map<String, String> configuration) {
        // do nothing on purpose
    }

    private void addIdStatusPair(final String key, final String status) {
        this.dataMap.put(key, status);
    }

    @Override
    public String getValue(final String key) {
        return this.dataMap.getOrDefault(key, "DNE");
    }

    @Override
    public boolean keyExists(final String key) {
        return this.dataMap.containsKey(key);
    }

    @Override
    public void loadResource(final String resource) throws IOException {
        Files.newBufferedReader(Paths.get(resource))
                .lines()
                .forEach(line -> {
                    final String[] tokens = line.split(",", 2);
                    addIdStatusPair(tokens[0], tokens[1]);
                });
    }

    @Override
    public String getConnectorType() {
        return "simple";
    }

    @Override
    public Stream<String> getAllKeys() {
        return this.dataMap.entrySet().stream().map(Map.Entry::getKey);
    }
}
