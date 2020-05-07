package dewilson.projects.lookup.perf;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Runner {

    public static void main(String[] args) throws Exception {

        final int REQUESTS = 10000;
        final int THREADS = 8;

        final ExecutorService executor = Executors.newFixedThreadPool(THREADS);

        final StopWatch sw = new StopWatch();
        sw.start();

        for (int i = 0; i < THREADS; i++) {
            executor.submit(new Task(REQUESTS));
        }

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        sw.stop();

        System.out.println(sw.getTime(TimeUnit.MILLISECONDS));
    }

    static class Task implements Callable<Integer> {
        private final int requests;

        public Task(int requests) {
            this.requests = requests;
        }

        public Integer call() throws Exception {
            for (int i = 0; i < this.requests; i++) {
                final Content content = Request.Get(
                        new URIBuilder("http://0.0.0.0:8888/getStatus").setParameter("id", "a").build())
                        .execute().returnContent();
            }
            return 50000;
        }

    }

}
