package dewilson.projects.lookup.service;

import dewilson.projects.lookup.support.DefaultSupportTypes;
import dewilson.projects.lookup.support.SimpleSupport;
import dewilson.projects.lookup.support.Support;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleMapLookUpService implements LookUpService {

    private final Map<String, String> idToStatusMap;
    private final Support valueSupport;
    private final Support filterSupport;

    public SimpleMapLookUpService() {
        this.idToStatusMap = new ConcurrentHashMap<>();
        this.valueSupport = new SimpleSupport(DefaultSupportTypes.VALUE);
        this.filterSupport = new SimpleSupport(DefaultSupportTypes.FILTER);
    }

    private void addIdStatusPair(final String id, final String status) {
        this.valueSupport.addSupport(status);
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
    public Support getValueSupport() {
        return this.valueSupport;
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
    public String getServiceType() {
        return "simple";
    }
}
