package dewilson.projects.lookup.service;

import dewilson.projects.lookup.support.SimpleSupport;
import dewilson.projects.lookup.support.Support;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleMapLookUpService implements LookUpService {

    private final Map<String, String> idToStatusMap;
    private final Support statusSupport;
    private final Support filterSupport;

    public SimpleMapLookUpService() {
        this.idToStatusMap = new ConcurrentHashMap<>();
        this.statusSupport = new SimpleSupport();
        this.filterSupport = new SimpleSupport();
    }

    private void addIdStatusPair(final String id, final String status) {
        this.statusSupport.addSupportedType(status);
        this.idToStatusMap.put(id, status);
    }

    @Override
    public String getStatus(final String id) {
        return this.idToStatusMap.getOrDefault(id, "DNE");
    }

    @Override
    public boolean idExists(final String id) {
        return this.idToStatusMap.containsKey(id);
    }

    @Override
    public Support getStatusSupport() {
        return this.statusSupport;
    }

    @Override
    public OutputStream getFilter(String type) {
        throw new UnsupportedOperationException();
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
    public String getType() {
        return "simple";
    }
}
