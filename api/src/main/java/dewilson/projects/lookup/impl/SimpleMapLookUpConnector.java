package dewilson.projects.lookup.impl;

import dewilson.projects.lookup.api.connector.LookUpConnector;
import dewilson.projects.lookup.api.support.DefaultSupportTypes;
import dewilson.projects.lookup.api.support.Support;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleMapLookUpConnector implements LookUpConnector {

    private final Map<String, String> idToStatusMap;
    private final Support filterSupport;

    public SimpleMapLookUpConnector() {
        this.idToStatusMap = new ConcurrentHashMap<>();
        this.filterSupport = new SimpleSupport(DefaultSupportTypes.FILTER);
    }

    @Override
    public void initialize(final Map<String, String> configuration) {
        // do nothing on purpose
    }

    private void addIdStatusPair(final String id, final String status) {
        this.idToStatusMap.put(id, status);
    }

    @Override
    public String getValue(final String id) {
        return this.idToStatusMap.getOrDefault(id, "DNE");
    }

    @Override
    public boolean idExists(final String id) {
        return this.idToStatusMap.containsKey(id);
    }

    @Override
    public InputStream getFilter(final String type) {
        final byte[] returnBytes = new byte[1024 * 512];
        return new ByteArrayInputStream(returnBytes);
    }

    @Override
    public Support getFilterSupport() {
        return this.filterSupport;
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
}
