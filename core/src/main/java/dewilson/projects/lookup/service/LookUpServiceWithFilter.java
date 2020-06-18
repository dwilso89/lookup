package dewilson.projects.lookup.service;

import dewilson.projects.lookup.connector.LookUpConnector;
import dewilson.projects.lookup.connector.LookUpConnectorFactory;
import dewilson.projects.lookup.filter.FilterManager;
import dewilson.projects.lookup.filter.FilterResult;
import dewilson.projects.lookup.support.Support;

import java.io.InputStream;
import java.util.Map;
import java.util.stream.Stream;

public class LookUpServiceWithFilter implements LookUpService {

    private FilterManager filterManager;
    private LookUpConnector lookUpConnector;

    public LookUpServiceWithFilter(final Map<String, String> configuration) {
        this.lookUpConnector = LookUpConnectorFactory.getLookUpConnector(configuration);
        this.filterManager = new FilterManager(configuration, lookUpConnector);
    }

    public InputStream getFilter(final String type) {
        return this.filterManager.getFilter(type);
    }

    public Support getFilterSupport() {
        return this.filterManager.getFilterSupport();
    }

    @Override
    public boolean keyExists(String key) {
        final FilterResult filterResult = this.filterManager.test(key);
        switch (filterResult) {
            case MAY_EXIST:
                return this.lookUpConnector.keyExists(key);
            case DOES_NOT_EXIST:
                return false;
            case EXISTS:
                return true;
            default:
                throw new RuntimeException(String.format("Unknown filter result [%s] for key [%s]", filterResult, key));
        }
    }

    @Override
    public String getValue(final String key) {
        final FilterResult filterResult = this.filterManager.test(key);
        switch (filterResult) {
            case EXISTS:
            case MAY_EXIST:
                return this.lookUpConnector.getValue(key);
            case DOES_NOT_EXIST:
                return "DNE";
            default:
                throw new RuntimeException(String.format("Unknown filter result [%s] for key [%s]", filterResult, key));
        }
    }

    @Override
    public Stream<String> getAllKeys() {
        return this.lookUpConnector.getAllKeys();
    }

}
