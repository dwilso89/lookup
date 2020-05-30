package dewilson.projects.lookup.filter.api;

public interface MembershipFilter<T> extends Filter {

    boolean contains(final T t);

}
