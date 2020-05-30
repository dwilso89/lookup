package dewilson.projects.lookup.impl;

import dewilson.projects.lookup.api.support.Support;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class SimpleSupport implements Support {

    private final Set<String> supportTypes;
    private final String type;

    public SimpleSupport(final String type) {
        this.supportTypes = new HashSet<>();
        this.type = type;
    }
    @Override
    public String getSupportType() {
        return this.type;
    }

    @Override
    public void addSupport(final String type) {
        this.supportTypes.add(type);
    }

    @Override
    public Set<String> getSupport() {
        return Collections.unmodifiableSet(this.supportTypes);
    }

    @Override
    public boolean supports(final String type) {
        return this.supportTypes.contains(type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleSupport that = (SimpleSupport) o;
        return Objects.equals(supportTypes, that.supportTypes) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(supportTypes, type);
    }
}
