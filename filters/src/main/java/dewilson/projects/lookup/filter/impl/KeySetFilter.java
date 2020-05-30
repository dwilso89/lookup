package dewilson.projects.lookup.filter.impl;

import dewilson.projects.lookup.filter.api.MembershipFilter;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class KeySetFilter implements MembershipFilter<String> {

    private final Set<String> keySet;

    public KeySetFilter() {
        this.keySet = new HashSet<>();
    }

    @Override
    public boolean contains(final String key) {
        return this.keySet.contains(key);
    }


    @Override
    public String getType() {
        return "keyset";
    }

    @Override
    public void write(final OutputStream os) throws IOException {
        final DataOutputStream dos = new DataOutputStream(os);
        for (final String key : keySet) {
            dos.writeUTF(key);
        }
    }

    @Override
    public KeySetFilter read(final InputStream is) throws IOException {
        final DataInputStream dis = new DataInputStream(is);
        while (dis.available() != 0) {
            this.keySet.add(dis.readUTF());
        }
        return this;
    }

}
