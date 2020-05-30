package dewilson.projects.lookup.filter.api;

public interface ApproximateMembershipFilter extends Filter<byte[]> {

    boolean probablyExists(final byte[] member);

}
