package dewilson.projects.lookup.jmh;

import com.google.common.collect.Maps;
import dewilson.projects.lookup.connector.BloomMapLookUpConnector;
import dewilson.projects.lookup.connector.LookUpConnector;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Map;

@State(Scope.Thread)
public class JMHTest {

    @State(Scope.Thread)
    public static class MyState {

        final LookUpConnector lookup = new BloomMapLookUpConnector();

        @Setup(Level.Trial)
        public void doSetup() throws Exception {
            final Map<String, String> map = Maps.newHashMap();
            map.put("lookUp.key.col", "0");
            map.put("lookUp.val.col", "4");
            map.put("lookUp.work.dir", "/tmp/bloomBig");
            map.put("lookUp.resourceType", "csv");
            this.lookup.initialize(map);
            this.lookup.loadResource("/tmp/GOOG_2020.csv");
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.All})
    public boolean exists(MyState myState) throws Exception {
       return myState.lookup.keyExists("2020-20-05");
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(JMHTest.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }


}

