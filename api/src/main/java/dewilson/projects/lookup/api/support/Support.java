package dewilson.projects.lookup.api.support;

import java.util.Set;

public interface Support {

    String getSupportType();

    void addSupport(String type);

    Set<String> getSupport();

    boolean supports(String type);

}
