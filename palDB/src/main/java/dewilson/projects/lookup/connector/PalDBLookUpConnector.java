package dewilson.projects.lookup.connector;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.linkedin.paldb.api.PalDB;
import com.linkedin.paldb.api.StoreReader;
import com.linkedin.paldb.api.StoreWriter;
import dewilson.projects.lookup.api.connector.LookUpConnector;
import dewilson.projects.lookup.api.support.DefaultSupportTypes;
import dewilson.projects.lookup.api.support.Support;
import dewilson.projects.lookup.filter.HadoopApproximateMembershipFilter;
import dewilson.projects.lookup.filter.api.ApproximateMembershipFilter;
import dewilson.projects.lookup.filter.api.FilterFactory;
import dewilson.projects.lookup.filter.impl.GuavaApproximateMembershipFilter;
import dewilson.projects.lookup.filter.impl.ScalaApproximateMembershipFilter;
import dewilson.projects.lookup.impl.CSVKVReader;
import dewilson.projects.lookup.impl.SimpleSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PalDBLookUpConnector implements LookUpConnector {

    private static final Logger LOG = LoggerFactory.getLogger(PalDBLookUpConnector.class);

    private final Support filterSupport;
    private final Map<String, String> config;
    private StoreReader[] readers;
    private Map<String, File> filterFiles;
    private ApproximateMembershipFilter activeApproximateMembershipFilter;
    private int partitions;

    public PalDBLookUpConnector() {
        this.filterSupport = new SimpleSupport(DefaultSupportTypes.FILTER);
        this.config = Maps.newHashMap();
        this.filterFiles = Maps.newConcurrentMap();
    }

    @Override
    public void initialize(final Map<String, String> config) {
        this.config.putAll(config);
        this.partitions = Integer.parseInt(
                this.config.getOrDefault(
                        Configuration.PARTITION_COUNT_KEY,
                        Configuration.DEFAULT_PARTITION_COUNT));
        this.readers = new StoreReader[this.partitions];

    }

    @Override
    public boolean idExists(final String id) {
        if (this.activeApproximateMembershipFilter == null ||
                this.activeApproximateMembershipFilter.probablyExists(id.getBytes(Charsets.UTF_8))) {
            return !getValue(id).equals("DNE");
        }

        return false;
    }

    @Override
    public String getValue(final String id) {
        if (this.activeApproximateMembershipFilter == null ||
                this.activeApproximateMembershipFilter.probablyExists(id.getBytes(Charsets.UTF_8))) {
            final Object value;
            final StoreReader reader = getStoreReaderForId(id);
            synchronized (reader) {
                value = reader.get(id);
            }
            if (value != null) {
                return value.toString();
            }
        }
        return "DNE";
    }

    private StoreReader getStoreReaderForId(final String id) {
        if (this.partitions > 1) {
            return this.readers[Math.abs(id.hashCode() % this.partitions)];
        } else {
            return this.readers[0];
        }
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
        final String resourceType = this.config.getOrDefault("lookUp.connector.resource.type", "palDB");

        switch (resourceType) {
            case "palDB":
                this.readers[0] = PalDB.createReader(new File(resource));
                break;
            case "csv":
                final File palDBFile = new File(getWorkDir() + "/palDB");
                if (palDBFile.exists()) {
                    Files.walk(palDBFile.toPath())
                            .sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(File::delete);
                }

                final List<StoreWriter> writers = Lists.newArrayList();
                if (this.partitions > 1) {
                    for (int i = 0; i < partitions; i++) {
                        writers.add(PalDB.createWriter(new File(palDBFile.getAbsolutePath() + "/partition-" + i + ".store")));
                    }

                    new CSVKVReader().initialize(resource, this.config).getKVStream().forEach(pair -> {
                        final int hash = Math.abs(pair.getKey().hashCode() % this.partitions);
                        writers.get(hash).put(pair.getKey(), pair.getValue());
                    });

                    writers.forEach(StoreWriter::close);

                    for (int i = 0; i < this.partitions; i++) {
                        this.readers[i] = PalDB.createReader(
                                new File(palDBFile.getAbsolutePath() + "/partition-" + i + ".store"));
                    }
                } else {
                    final StoreWriter writer = PalDB.createWriter(palDBFile);
                    new CSVKVReader().initialize(resource, this.config).getKVStream()
                            .forEach(pair -> writer.put(pair.getKey(), pair.getValue()));
                    writer.close();
                    this.readers[0] = PalDB.createReader(palDBFile);
                }
                break;
            default:
                throw new IllegalArgumentException("PalDB does not support resource type [" + resourceType + "]");
        }

        loadFilters();
    }

    @Override
    public String getConnectorType() {
        return "palDB-1.2.0";
    }

    private String getWorkDir() {
        return this.config.getOrDefault(Configuration.WORK_DIR_KEY, Configuration.DEFAULT_WORK_DIR);
    }

    private void loadFilters() throws IOException {
        final String activeFilterType = this.config.getOrDefault(Configuration.ACTIVE_TYPE_KEYS, Configuration.DEFAULT_ACTIVE_FILTER);
        LOG.info("Creating filters... looking for active type [{}]", activeFilterType);

        for (final String filterType : Splitter.on(",")
                .omitEmptyStrings()
                .trimResults()
                .split(this.config.getOrDefault(Configuration.FILTERS_KEY, Configuration.DEFAULT_FILTERS))) {
            LOG.info("Creating filter [{}]", filterType);
            this.config.put("", String.valueOf(getAllKeys().count()));
            final ApproximateMembershipFilter approximateMembershipFilter = FilterFactory.getApproximateMembershipFilter(filterType, this.config, getAllKeys());

            final File bloomFilterFile = new File(getWorkDir() + "/" + filterType);
            try (final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(bloomFilterFile))) {
                approximateMembershipFilter.write(bos);
                bos.flush();
            }
            this.filterFiles.put(filterType, bloomFilterFile);
            this.filterSupport.addSupport(filterType);

            // set active filter
            if (!Strings.isNullOrEmpty(activeFilterType) && activeFilterType.equals(filterType)) {
                LOG.info("Setting active filter to filter [{}]", activeFilterType);
                this.activeApproximateMembershipFilter = approximateMembershipFilter;
            }
        }
    }

    private Stream<byte[]> getAllKeys() {
        Stream<byte[]> keyStream = new ArrayList<byte[]>().stream();
        for (StoreReader reader : this.readers) {
            keyStream = Streams.concat(keyStream, StreamSupport.stream(reader.keys().spliterator(), false)
                    .map(str -> str.toString().getBytes(Charsets.UTF_8)));
        }

        return keyStream;
    }


}