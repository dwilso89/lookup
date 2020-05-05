package dewilson.projects.lookup.service;

import dewilson.projects.lookup.support.Support;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public interface LookUpService {

    default void initialize(Map<String, String> config) {
        // no-op
    }

    void loadResource(String resource) throws IOException;

    String getType();

    String getStatus(String id);

    Support getStatusSupport();

    OutputStream getFilter(String type);

    Support getFilterSupport();

}
