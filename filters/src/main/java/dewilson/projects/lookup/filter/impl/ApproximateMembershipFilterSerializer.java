package dewilson.projects.lookup.filter.impl;

import com.google.common.collect.Maps;
import dewilson.projects.lookup.filter.api.ApproximateMembershipFilter;
import dewilson.projects.lookup.filter.api.Filter;
import dewilson.projects.lookup.filter.api.FilterFactory;

import java.io.*;

public class ApproximateMembershipFilterSerializer {

    private ApproximateMembershipFilterSerializer() {
        // empty
    }

    public static void write(final Filter<?> filter, final OutputStream os) throws IOException {
        final DataOutputStream dos = new DataOutputStream(os);
        dos.writeUTF(filter.getType());
        filter.write(os);
    }

    public static ApproximateMembershipFilter read(final InputStream is) throws IOException {
        final DataInputStream dis = new DataInputStream(is);
        final String type = dis.readUTF();
        return FilterFactory.getApproximateMembershipFilter(type, Maps.newHashMap()).read(is);
    }

}
