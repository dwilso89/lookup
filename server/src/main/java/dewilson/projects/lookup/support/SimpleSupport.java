package dewilson.projects.lookup.support;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class SimpleSupport implements Support {

    private final Set<String> supportTypes;

    public SimpleSupport() {
        this.supportTypes = new HashSet<>();
    }

    @Override
    public void addSupportedType(final String type) {
        this.supportTypes.add(type);
    }

    @Override
    public Set<String> getSupportedTypes() {
        return Collections.unmodifiableSet(this.supportTypes);
    }

    @Override
    public boolean supportsType(final String type) {
        return this.supportTypes.contains(type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleSupport that = (SimpleSupport) o;
        return Objects.equals(getSupportedTypes(), that.getSupportedTypes());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getSupportedTypes());
    }

}
