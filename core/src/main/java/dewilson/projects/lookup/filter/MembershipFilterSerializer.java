package dewilson.projects.lookup.filter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;

public class MembershipFilterSerializer {

    private MembershipFilterSerializer() {
        // empty
    }

    public static void write(final MembershipFilter membershipFilter, final OutputStream os) throws IOException {
        final DataOutputStream dos = new DataOutputStream(os);
        dos.writeUTF(membershipFilter.getType());
        membershipFilter.write(os);
    }

    public static MembershipFilter read(final InputStream is) throws IOException {
        final DataInputStream dis = new DataInputStream(is);
        final String type = dis.readUTF();
        return FilterFactory.getMembershipFilter(type, new HashMap<>()).read(is);
    }

}
