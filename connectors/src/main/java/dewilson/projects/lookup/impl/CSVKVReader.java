package dewilson.projects.lookup.impl;

import dewilson.projects.lookup.api.reader.KVReader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

public class CSVKVReader implements KVReader<String, String> {

    private int keyCol;
    private int valCol;
    private String resource;

    @Override
    public CSVKVReader initialize(final String resource, final Map<String, String> configuration) {
        this.keyCol = Integer.parseInt(configuration.getOrDefault("kvreader.csv.key.col", "0"));
        this.valCol = Integer.parseInt(configuration.getOrDefault("kvreader.csv.val.col", "1"));
        this.resource = resource;
        return this;
    }

    @Override
    public Stream<Pair> getKVStream() throws IOException {
        return Files.lines(Path.of(this.resource)).map(line -> {
            final String[] lineSplit = line.split(",");
            return new Pair(lineSplit[this.keyCol], lineSplit[this.valCol]);
        });
    }

    @Override
    public String getType() {
        return "csv";
    }

}
