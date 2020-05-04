package dewilson.projects.lookup.service;

import dewilson.projects.lookup.support.Support;

import java.io.IOException;
import java.io.OutputStream;

public interface LookUpService {

    String getStatus(String id);

    Support getStatusSupport();

    OutputStream getFilter(String type);

    Support getFilterSupport();

    void loadResource(String resource) throws IOException;

    String getType();
}
