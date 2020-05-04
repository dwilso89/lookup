package dewilson.projects.lookup.support;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class SupportSerializer extends JsonSerializer<Support> {


    @Override
    public void serialize(final Support support, final JsonGenerator jG, final SerializerProvider provider) throws IOException {
        jG.writeStartObject();
        jG.writeArrayFieldStart("abc");

        for (final String type : support.getSupportedTypes()) {
            jG.writeString(type);
        }

        jG.writeEndArray();
        jG.writeEndObject();
    }

}
