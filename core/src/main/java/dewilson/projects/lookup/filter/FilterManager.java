package dewilson.projects.lookup.filter;

import dewilson.projects.lookup.connector.LookUpConnector;
import dewilson.projects.lookup.support.DefaultSupportTypes;
import dewilson.projects.lookup.support.SimpleSupport;
import dewilson.projects.lookup.support.Support;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FilterManager {

    private static final Logger LOG = LoggerFactory.getLogger(FilterManager.class);

    private static final String FILTERS_KEY = "lookUp.filters";
    private static final String ACTIVE_TYPE_KEYS = "lookUp.filter.active.type";
    private static final String DEFAULT_FILTERS = "";
    private static final String DEFAULT_ACTIVE_FILTER = "";
    private static final String WORK_DIR_KEY = "lookUp.work.dir";
    private static final String DEFAULT_WORK_DIR = "/tmp/";

    private final Map<String, File> filterFiles;
    private final Support filterSupport;
    private final String workDir;
    private MembershipFilter activeMembershipFilter;

    public FilterManager(final Map<String, String> configuration, final LookUpConnector lookUpConnector) {
        this.filterFiles = new ConcurrentHashMap<>();
        this.filterSupport = new SimpleSupport(DefaultSupportTypes.FILTER);
        this.workDir = configuration.getOrDefault(WORK_DIR_KEY, DEFAULT_WORK_DIR);

        try {
            loadFilters(configuration, lookUpConnector);
        } catch (final IOException ioe) {
            throw new RuntimeException("Unable to load filters.", ioe);
        }
    }

    public FilterResult test(final String key) {
        return this.activeMembershipFilter.test(key);
    }

    public Support getFilterSupport(){
        return this.filterSupport;
    }

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


    private void loadFilters(final Map<String, String> configuration, final LookUpConnector lookUpConnector) throws IOException {
        final String activeFilterType = configuration.getOrDefault(ACTIVE_TYPE_KEYS, DEFAULT_ACTIVE_FILTER);
        LOG.info("Creating filters... looking for active type [{}]", activeFilterType);

        for (final String filterType : configuration.getOrDefault(FILTERS_KEY, DEFAULT_FILTERS).split(",")) {
            if (filterType.isEmpty()) {
                LOG.info("Omitting empty filter type [{}]", filterType);

            }
            LOG.info("Creating filter [{}]", filterType);
            configuration.put("", String.valueOf(lookUpConnector.getAllKeys().count()));
            final MembershipFilter approximateMembershipFilter = FilterFactory.getMembershipFilter(filterType, configuration, lookUpConnector.getAllKeys());

            final File bloomFilterFile = new File(this.workDir + "/" + filterType);
            try (final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(bloomFilterFile))) {
                approximateMembershipFilter.write(bos);
                bos.flush();
            }
            this.filterFiles.put(filterType, bloomFilterFile);
            this.filterSupport.addSupport(filterType);

            // set active filter
            if (activeFilterType != null && !activeFilterType.isEmpty() && activeFilterType.equals(filterType)) {
                LOG.info("Setting active filter to filter [{}]", activeFilterType);
                this.activeMembershipFilter = approximateMembershipFilter;
            }
        }
    }


}
