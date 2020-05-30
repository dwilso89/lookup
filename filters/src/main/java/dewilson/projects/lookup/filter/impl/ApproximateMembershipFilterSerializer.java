package dewilson.projects.lookup.filter.impl;

import dewilson.projects.lookup.filter.api.ApproximateMembershipFilter;
import dewilson.projects.lookup.filter.api.Filter;

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

        switch (type) {
            case "guava":
                return new GuavaApproximateMembershipFilter.Builder().build().read(is);
            case "scala":
                return new ScalaApproximateMembershipFilter.Builder().build().read(is);
            default:
                throw new UnsupportedOperationException(String.format("Unable to deserialize filter of type [%s]", type));
        }

    }

}
