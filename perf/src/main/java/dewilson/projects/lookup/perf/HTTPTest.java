package dewilson.projects.lookup.perf;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.protocol.BasicHttpContext;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.net.URIBuilder;

import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HTTPTest {

    public static void main(final String[] args) throws Exception {

        final int REQUESTS = 100000;
        final int THREADS = 1;

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
        private final URI uri;
        private final CloseableHttpClient httpclient;
        private final HttpGet httpGet;

        Task(int requests) throws Exception {

            // default
            // this.httpclient = HttpClients.createDefault();

            // minimal
            this.httpclient = HttpClients.createMinimal();

            this.requests = requests;

            this.uri = new URIBuilder("http://localhost:8888/exists").setParameter("id", "2025-05-05").build();

            // this.uri = new URIBuilder("http://localhost:8888/getSupportedFilters").build();

            this.httpGet = new HttpGet(this.uri);
        }

        public Integer call() {
            HttpContext context = new BasicHttpContext();
            try {
                for (int i = 0; i < this.requests; i++) {
                    try (final CloseableHttpResponse response = this.httpclient.execute(this.httpGet, context)) {
                        EntityUtils.toString(response.getEntity());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return requests;
        }

    }

}
