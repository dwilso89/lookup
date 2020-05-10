package dewilson.projects.lookup.support;

import java.util.Set;

public interface Support {

    String getSupportType();

    void addSupport(String type);

    Set<String> getSupport();

    boolean supports(String type);

}
