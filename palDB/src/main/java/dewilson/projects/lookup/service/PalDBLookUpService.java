package dewilson.projects.lookup.service;

import com.linkedin.paldb.api.PalDB;
import com.linkedin.paldb.api.StoreReader;
import com.linkedin.paldb.api.StoreWriter;
import dewilso.projects.lookup.filter.BloomFilter;
import dewilso.projects.lookup.filter.GuavaBloomFilter;
import dewilson.projects.lookup.reader.CSVKVReader;
import dewilson.projects.lookup.support.DefaultSupportTypes;
import dewilson.projects.lookup.support.SimpleSupport;
import dewilson.projects.lookup.support.Support;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.StreamSupport;

public class PalDBLookUpService implements LookUpService {

    private final Support valueSupport;
    private final Support filterSupport;
    private final Map<String, String> config;
    private StoreReader reader;
    private BloomFilter<String> bloomFilter;
    private File bloomFile;
    private String workDir = "/tmp";
    private long bloomTime = 0L;

    public PalDBLookUpService() {
        this.valueSupport = new SimpleSupport(DefaultSupportTypes.VALUE);
        this.filterSupport = new SimpleSupport(DefaultSupportTypes.FILTER);
        this.config = new HashMap<>();
    }

    @Override
    public void initialize(final Map<String, String> config) {
        this.config.putAll(config);
        this.workDir = this.config.getOrDefault("lookUp.work.dir", this.workDir);
    }

    @Override
    public boolean idExists(final String id) {
        long start = System.currentTimeMillis();
        if (this.bloomFilter == null || this.bloomFilter.probablyExists(id)) {
            return !getValue(id).equals("DNE");
        }
        bloomTime += System.currentTimeMillis() - start;

        return false;
    }

    @Override
    public String getValue(final String id) {
        if (this.bloomFilter == null || this.bloomFilter.probablyExists(id)) {
            final Object value = this.reader.get(id);
            if (value != null) {
                return value.toString();
            }
        }
        return "DNE";
    }

    @Override
    public Support getValueSupport() {
        return this.valueSupport;
    }

    @Override
    public InputStream getFilter(final String type) {
        if (this.filterSupport.getSupport().contains(type)) {
            try {
                return new BufferedInputStream(new FileInputStream(this.bloomFile));
            } catch (IOException ioe) {
                throw new RuntimeException(String.format("Unable to serialize filter of type [%s]", type));
            }
        }

        throw new UnsupportedOperationException(
                String.format("PalDB lookup service does not support filter of type [%s]", type));
    }

    @Override
    public Support getFilterSupport() {
        return this.filterSupport;
    }

    @Override
    public void loadResource(final String resource) throws IOException {
        final String resourceType = this.config.getOrDefault("lookUp.resourceType", "palDB");

        final String palDBFile;
        switch (resourceType) {
            case "palDB":
                palDBFile = resource;
                break;
            case "csv":
                palDBFile = this.config.getOrDefault("lookUp.work.dir", "/tmp") + "/palDB";
                final StoreWriter writer = PalDB.createWriter(new File(palDBFile));
                new CSVKVReader().initialize(resource, this.config).getKVStream()
                        .forEach(pair -> writer.put(pair.getKey(), pair.getValue()));
                writer.close();
                break;
            default:
                throw new IllegalArgumentException("PalDB does not support resource type [" + resourceType + "]");
        }

        this.reader = PalDB.createReader(new File(palDBFile));

        final String createBloom = this.config.get("lookUp.filter.guavaBloom");
        if (Boolean.valueOf(createBloom)) {
            System.out.println(String.format("Creating filter [%s]", "guavaBloom"));
            this.bloomFilter = new GuavaBloomFilter.Builder()
                    .elements(
                            StreamSupport.stream(this.reader.keys().spliterator(), false))
                    .build();
            this.filterSupport.addSupport("lookUp.filter.guavaBloom");
            this.bloomFile = new File(this.workDir + "/" + "lookUp.filter.guavaBloom");
            this.bloomFilter.write(new BufferedOutputStream(new FileOutputStream(this.bloomFile)));
        }

        this.reader.iterable().forEach(pair -> this.valueSupport.addSupport(pair.getValue().toString()));
        this.valueSupport.addSupport("DNE");
    }

    @Override
    public String getServiceType() {
        return "palDB-1.2.0";
    }

}