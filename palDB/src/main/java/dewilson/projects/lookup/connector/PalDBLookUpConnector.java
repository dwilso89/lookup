package dewilson.projects.lookup.connector;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.linkedin.paldb.api.PalDB;
import com.linkedin.paldb.api.StoreReader;
import com.linkedin.paldb.api.StoreWriter;
import dewilso.projects.lookup.filter.BloomFilter;
import dewilso.projects.lookup.filter.GuavaBloomFilter;
import dewilso.projects.lookup.filter.ScalaBloomFilter;
import dewilson.projects.lookup.reader.CSVKVReader;
import dewilson.projects.lookup.reader.KVReader;
import dewilson.projects.lookup.support.DefaultSupportTypes;
import dewilson.projects.lookup.support.SimpleSupport;
import dewilson.projects.lookup.support.Support;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;

public class PalDBLookUpConnector implements LookUpConnector {

    private final Support filterSupport;
    private final Map<String, String> config;
    private final List<StoreReader> readers;
    private StoreReader reader;
    private Map<String, File> filterFiles;
    private BloomFilter<String> activeBloomFilter;

    private String workDir = "/tmp";
    private String activeFilterType = "";
    private boolean partition = false;
    private int partitions = 1;
    private boolean guavaFilter = false;
    private boolean scalaFilter = false;

    private AtomicLong guavaBloomTime = new AtomicLong(0L);

    public PalDBLookUpConnector() {
        this.filterSupport = new SimpleSupport(DefaultSupportTypes.FILTER);
        this.config = Maps.newHashMap();
        this.readers = Lists.newArrayList();
        this.filterFiles = Maps.newConcurrentMap();
    }

    @Override
    public void initialize(final Map<String, String> config) {
        this.config.putAll(config);
        this.workDir = this.config.getOrDefault("lookUp.work.dir", this.workDir);
        this.partition = Boolean.valueOf(this.config.getOrDefault("lookUp.partition", "false"));
        this.partitions = Integer.parseInt(this.config.getOrDefault("lookUp.partitions", "1"));
        this.guavaFilter = Boolean.valueOf(this.config.getOrDefault("lookUp.filter.guavaBloom", "false"));
        this.scalaFilter = Boolean.valueOf(this.config.getOrDefault("lookUp.filter.scalaBloom", "false"));
        this.activeFilterType = this.config.getOrDefault("lookUp.filter.activeType", "");
    }

    @Override
    public boolean idExists(final String id) {
        long start = System.currentTimeMillis();
        if (this.activeBloomFilter == null || this.activeBloomFilter.probablyExists(id)) {
            return !getValue(id).equals("DNE");
        }
        this.guavaBloomTime.getAndAdd(System.currentTimeMillis() - start);

        return false;
    }

    @Override
    public String getValue(final String id) {
        if (this.activeBloomFilter == null || this.activeBloomFilter.probablyExists(id)) {
            final Object value;
            if (this.partition) {
                final StoreReader reader = this.readers.get(Math.abs(id.hashCode() % this.partitions));
                synchronized (reader) {
                    value = reader.get(id);
                }
            } else {
                value = this.reader.get(id);
            }
            if (value != null) {
                return value.toString();
            }
        }
        return "DNE";
    }

    @Override
    public InputStream getFilter(final String type) {
        if (this.filterSupport.getSupport().contains(type)) {
            try {
                return new BufferedInputStream(new FileInputStream(this.filterFiles.get(type)));
            } catch (final IOException ioe) {
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

        switch (resourceType) {
            case "palDB":
                this.reader = PalDB.createReader(new File(resource));
                if (this.guavaFilter) {
                    System.out.println(String.format("Creating filter [%s]", "guavaBloom"));
                    final BloomFilter<String> bloomFilter = new GuavaBloomFilter.Builder()
                            .elements(
                                    StreamSupport.stream(this.reader.keys().spliterator(), false))
                            .build();
                    this.filterSupport.addSupport("guavaBloom");
                    final File guavaBloomFile = new File(this.workDir + "/" + "guavaBloom");
                    this.filterFiles.put("guavaBloom", guavaBloomFile);
                    bloomFilter.write(new BufferedOutputStream(new FileOutputStream(guavaBloomFile)));
                    this.filterFiles.put("guavaBloom", guavaBloomFile);
                    if (this.activeFilterType.equals("guavaBloom")) {
                        this.activeBloomFilter = bloomFilter;
                    }
                }
                break;
            case "csv":
                final File palDBFile = new File(this.config.getOrDefault("lookUp.work.dir", "/tmp") + "/palDB");
                if (palDBFile.exists()) {
                    Files.walk(palDBFile.toPath())
                            .sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(File::delete);
                }

                final List<StoreWriter> writers = Lists.newArrayList();
                if (this.partition) {
                    for (int i = 0; i < this.partitions; i++) {
                        writers.add(PalDB.createWriter(new File(palDBFile.getAbsolutePath() + "/partition-" + i + ".store")));
                    }

                    new CSVKVReader().initialize(resource, this.config).getKVStream().forEach(pair -> {
                        final int hash = Math.abs(pair.getKey().hashCode() % partitions);
                        writers.get(hash).put(pair.getKey(), pair.getValue());
                    });

                    writers.forEach(StoreWriter::close);

                    for (int i = 0; i < this.partitions; i++) {
                        this.readers.add(
                                PalDB.createReader(
                                        new File(palDBFile.getAbsolutePath() + "/partition-" + i + ".store")));
                    }
                } else {
                    final StoreWriter writer = PalDB.createWriter(palDBFile);
                    new CSVKVReader().initialize(resource, this.config).getKVStream()
                            .forEach(pair -> writer.put(pair.getKey(), pair.getValue()));
                    writer.close();
                    this.reader = PalDB.createReader(palDBFile);
                }

                if (this.guavaFilter) {
                    System.out.println(String.format("Creating filter [%s]", "guavaBloom"));
                    final BloomFilter<String> bloomFilter = new GuavaBloomFilter.Builder()
                            .elements(new CSVKVReader().initialize(resource, this.config)
                                    .getKVStream().map(KVReader.Pair::getKey))
                            .build();
                    this.filterSupport.addSupport("guavaBloom");
                    final File guavaBloomFile = new File(this.workDir + "/" + "guavaBloom");
                    bloomFilter.write(new BufferedOutputStream(new FileOutputStream(guavaBloomFile)));
                    this.filterFiles.put("guavaBloom", guavaBloomFile);
                    if (this.activeFilterType.equals("guavaBloom")) {
                        System.out.println(String.format("Setting active filter to filter [%s]", "guavaBloom"));
                        this.activeBloomFilter = bloomFilter;
                    }
                }

                if (this.scalaFilter) {
                    System.out.println(String.format("Creating filter [%s]", "scalaBloom"));
                    final BloomFilter<String> bloomFilter = new ScalaBloomFilter.Builder()
                            .elements(new CSVKVReader().initialize(resource, this.config)
                                    .getKVStream().map(KVReader.Pair::getKey))
                            .build();
                    this.filterSupport.addSupport("scalaBloom");
                    final File scalaBloomFile = new File(this.workDir + "/" + "scalaBloom");
                    bloomFilter.write(new BufferedOutputStream(new FileOutputStream(scalaBloomFile)));
                    this.filterFiles.put("scalaBloom", scalaBloomFile);
                    if (this.activeFilterType.equals("scalaBloom")) {
                        System.out.println(String.format("Setting active filter to filter [%s]", "scalaBloom"));
                        this.activeBloomFilter = bloomFilter;
                    }
                }

                break;
            default:
                throw new IllegalArgumentException("PalDB does not support resource type [" + resourceType + "]");
        }

    }

    @Override
    public String getServiceType() {
        return "palDB-1.2.0";
    }

}