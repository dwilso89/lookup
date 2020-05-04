package dewilson.projects.lookup.support;

import java.util.Set;

//@JsonSerialize(using = SupportSerializer.class)
public interface Support {

    void addSupportedType(String type);

    Set<String> getSupportedTypes();

    boolean supportsType(String type);

}
