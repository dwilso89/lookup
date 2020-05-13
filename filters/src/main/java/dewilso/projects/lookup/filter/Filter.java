package dewilso.projects.lookup.filter;

public interface Filter<T> {

    boolean exists(T t);

}
