package dewilson.projects.lookup.connector;

import dewilson.projects.lookup.support.Support;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.stream.Stream;

public interface LookUpConnector {

    String getConnectorType();

    void initialize(Map<String, String> config);

    void loadResource(String resource) throws IOException;

    boolean keyExists(String key);

    String getValue(String key);

    Stream<String> getAllKeys();

}
