package dewilson.projects.lookup.service;

import java.util.stream.Stream;

public interface LookUpService {

    boolean keyExists(String key);

    Stream<String> getAllKeys();

    String getValue(String key);

}
