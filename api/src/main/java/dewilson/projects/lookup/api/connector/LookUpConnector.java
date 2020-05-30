package dewilson.projects.lookup.api.connector;


import dewilson.projects.lookup.api.support.Support;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public interface LookUpConnector {

    String getConnectorType();

    void initialize(Map<String, String> config);

    void loadResource(String resource) throws IOException;

    boolean idExists(String id);

    String getValue(String id);

    InputStream getFilter(String type) throws IOException;

    Support getFilterSupport();

}
