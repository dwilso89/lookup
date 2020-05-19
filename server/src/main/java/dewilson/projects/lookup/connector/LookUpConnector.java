package dewilson.projects.lookup.connector;

import dewilson.projects.lookup.support.Support;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public interface LookUpConnector {

    default void initialize(Map<String, String> config) {
        // no-op
    }

    void loadResource(String resource) throws IOException;

    String getServiceType();

    boolean idExists(String id);

    String getValue(String id);

    InputStream getFilter(String type) throws IOException;

    Support getFilterSupport();

}
