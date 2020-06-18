package dewilson.projects.lookup.perf;


import org.apache.commons.lang3.time.StopWatch;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class GenerateData {

    public static final Map<Integer, String> VALUES = new ConcurrentHashMap<>();

    public static void main(final String[] args) throws Exception {

        // Simple program to generated sorted key|value pairs of csv files.
        // Files can be concatenated after program execution in many ways such as:
        // 'for i in {0..999}; do cat /tmp/data/$i.csv >> /tmp/bigfile.csv; done'
        // The resulting CSV file can be used by any of the services supporting the CSVKVReader

        // KEYS - monotonically increasing data times with second granularity which guarantees sorted order
        // for easy Hadoop Bloom MapFile creation but also limits KEYS per TASK to 86400
        // VALUES - a 'random' selection from letters [a-e]
        VALUES.put(0, "a");
        VALUES.put(1, "b");
        VALUES.put(2, "c");
        VALUES.put(3, "d");
        VALUES.put(4, "e");

        // This will use 8 THREADS to generate a file for each TASK (1000) in the OUTPUT_DIR, each with 86400 KEYS.
        // A total of 86,400,000 K|V pairs will be generated.
        final int THREADS = 8;
        final int TASKS = 1000;
        final int KEYS = 86400;

        if(KEYS > 86400){
            throw new RuntimeException("KEYS cannot be larger than 86400");
        }

        final String OUTPUT_DIR = "/tmp/data";

        final ExecutorService executor = Executors.newFixedThreadPool(THREADS);

        final StopWatch sw = new StopWatch();
        sw.start();

        for (int i = 0, j = TASKS; i < TASKS; i++, j--) {
            final OutputStream os = new BufferedOutputStream(new FileOutputStream(new File(OUTPUT_DIR + i + ".csv")));
            executor.submit(new Task(KEYS, LocalDateTime.now().minusDays(j).truncatedTo(ChronoUnit.DAYS).toEpochSecond(ZoneOffset.UTC), os));

        }

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        sw.stop();

        System.out.println(sw.getTime(TimeUnit.MILLISECONDS));

    }

    static class Task implements Runnable {
        private final int limit;
        private final long start;
        private final OutputStream os;
        private final Random random = new Random();


        Task(int limit, long start, OutputStream os) {
            this.limit = limit;
            this.start = start;
            this.os = os;
        }

        public void run() {

            try {
                for (int i = 1; i <= this.limit; i++) {
                    final String key = LocalDateTime.ofEpochSecond(this.start + i, 0, ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME);
                    final String value = VALUES.get(random.nextInt(5));
                    this.os.write((key + "," + value + "\n").getBytes());

                }
                this.os.close();
            } catch (final IOException ioe) {
                throw new RuntimeException(ioe);
            }

        }

    }

}
