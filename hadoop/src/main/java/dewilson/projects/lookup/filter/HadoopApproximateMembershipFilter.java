package dewilson.projects.lookup.filter;

import com.google.common.base.Strings;
import dewilson.projects.lookup.filter.api.ApproximateMembershipFilter;
import dewilson.projects.lookup.filter.api.ApproximateMembershipFilterBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.util.bloom.DynamicBloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.*;
import java.util.Map;
import java.util.stream.Stream;

public class HadoopApproximateMembershipFilter implements ApproximateMembershipFilter {

    public static final String TYPE = "hadoop-2.10";

    private DynamicBloomFilter dynamicBloomFilter;
    private Key key = new Key();

    private HadoopApproximateMembershipFilter(final DynamicBloomFilter dynamicBloomFilter) {
        this.dynamicBloomFilter = dynamicBloomFilter;
    }

    @Override
    public boolean probablyExists(byte[] member) {
        this.key.set(member, 1.0F);
        return this.dynamicBloomFilter.membershipTest(this.key);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void write(final OutputStream os) throws IOException {
        this.dynamicBloomFilter.write(new DataOutputStream(os));
    }

    @Override
    public HadoopApproximateMembershipFilter read(InputStream is) throws IOException {
        this.dynamicBloomFilter.readFields(new DataInputStream(is));
        return this;
    }

    public static class Builder implements ApproximateMembershipFilterBuilder {

        private double errorRate = 0.005F;
        private int hashType = Hash.MURMUR_HASH;
        private int hashCount = 5;
        private Stream<byte[]> elements;
        private String resource;

        public Builder() {
            // empty on purpose
        }

        @Override
        public String getType() {
            return TYPE;
        }

        @Override
        public Builder initialize(final Map<String, String> configuration) {
            // if points to serialized filter
            final String resource = configuration.get("lookUp.filter.hadoop.resource");
            if (!Strings.isNullOrEmpty(resource)) {
                this.resource = resource;
                return this;
            } else {
                final String errorRateConfig = configuration.get("lookUp.filter.hadoop.errorRate");
                if (errorRateConfig != null) {
                    errorRate(Double.valueOf(errorRateConfig));
                }

                final String hashTypeConfig = configuration.get("lookUp.filter.hadoop.hashType");
                if (hashTypeConfig != null) {
                    hashType(Integer.valueOf(hashTypeConfig));
                }
                final String hashCountConfig = configuration.get("lookUp.filter.hadoop.hashCount");
                if (hashCountConfig != null) {
                    hashCount(Integer.valueOf(hashCountConfig));
                }

                return this;
            }
        }

        @Override
        public HadoopApproximateMembershipFilter build() {
            if (!Strings.isNullOrEmpty(this.resource)) {
                try {
                    return new HadoopApproximateMembershipFilter(null)
                            .read(new BufferedInputStream(new FileInputStream(new File(this.resource))));
                } catch (final IOException ioe) {
                    throw new RuntimeException(String.format("Unable to read hadoop bloom filter mapfile from [%s]", this.resource));
                }
            } else {

                final int nr = new Configuration().getInt(
                        CommonConfigurationKeysPublic.IO_MAPFILE_BLOOM_SIZE_KEY,
                        CommonConfigurationKeysPublic.IO_MAPFILE_BLOOM_SIZE_DEFAULT);

                final int vectorSize = (int) Math.ceil((double) (-this.hashCount * nr) /
                        Math.log(1.0 - Math.pow(this.errorRate, 1.0 / this.hashCount)));

                final DynamicBloomFilter filter = new DynamicBloomFilter(vectorSize, this.hashCount,
                        this.hashType, nr);


                this.elements.forEach(element -> filter.add(new Key(element)));

                return new HadoopApproximateMembershipFilter(filter);
            }
        }

        @Override
        public HadoopApproximateMembershipFilter.Builder elements(final Stream<byte[]> elements) {
            this.elements = elements;
            return this;
        }

        public HadoopApproximateMembershipFilter.Builder errorRate(final double errorRate) {
            this.errorRate = errorRate;
            return this;
        }

        public HadoopApproximateMembershipFilter.Builder hashCount(final int hashCount) {
            this.hashCount = hashCount;
            return this;
        }

        public HadoopApproximateMembershipFilter.Builder hashType(final int hashType) {
            this.hashType = hashType;
            return this;
        }

    }

}
