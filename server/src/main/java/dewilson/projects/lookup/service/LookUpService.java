package dewilson.projects.lookup.service;

import dewilson.projects.lookup.support.Support;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public interface LookUpService {

    default void initialize(Map<String, String> config) {
        // no-op
    }

    void loadResource(String resource) throws IOException;

    String getServiceType();

    boolean idExists(String id);

    String getValue(String id);

    Support getValueSupport();

    InputStream getFilter(String type) throws IOException;

    Support getFilterSupport();

}
