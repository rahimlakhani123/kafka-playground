package org.example.price.source;

import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Builder
@Slf4j
public class AsyncPriceFetcher implements PriceFetcher {
    private String url;
    private String host;
    private String apiKey;
    private Vertx vertx;
    private long initialDelay;
    private long scheduleFreq;
    private WebClient webClient;
    private AtomicLong taskId;

    private WebClient webClient() {
        if (webClient == null) {
            WebClientOptions options = new WebClientOptions()
                    .setSsl(true)
                    .setTrustAll(true);
            webClient = WebClient.create(vertx, options);
        }
        return webClient;
    }

    @Override
    public void fetchPrice(List<String> symbols) {
        if (taskId != null) {
            return;
        }
        var periodicTaskId = vertx.setPeriodic(initialDelay, scheduleFreq, internalId -> {
            log.info("Periodic task internal Id {}", internalId);
            fetch(symbols);
        });
        taskId = new AtomicLong(periodicTaskId);
    }

    public void stop() {
        vertx.cancelTimer(taskId.get());
        webClient().close();
        vertx.close();
    }

    private void fetch(List<String> symbols) {
        webClient().getAbs(url)
                .putHeader("x-rapidapi-host", host)
                .putHeader("x-rapidapi-key", apiKey)
                .addQueryParam("symbol", String.join(",", symbols))
                .send()
                .onSuccess(response -> {
                    log.info(response.bodyAsString());
                })
                .onFailure(err -> log.error("Error Observer", err));
    }
}
