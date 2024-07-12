package org.example;

import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import org.example.price.source.AsyncPriceFetcher;
import org.example.price.source.PriceFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("started");

        var vertx = Vertx.vertx();
        final PriceFetcher fetcher = AsyncPriceFetcher.builder()
                .url("https://real-time-finance-data.p.rapidapi.com/stock-quote")
                .host("real-time-finance-data.p.rapidapi.com")
                .apiKey("1aba7153a3mshea7ffa54724c50ap1ea4dejsn02bff4cc3ef6")
                .vertx(vertx)
                .initialDelay(1000)
                .scheduleFreq(5000)
                .build();

        fetcher.fetchPrice(List.of("AAPL:NASDAQ"));

       var timer = vertx.setTimer(60 * 1000, aLong -> {
            logger.info("Canceling Scheduled Task");
            fetcher.stop();
        });

       Runtime.getRuntime().addShutdownHook(new Thread(()-> {
           vertx.cancelTimer(timer);
       }));

    }

    private static void pullData(WebClient webclient, Vertx vertx) {

        var taskId = vertx.setPeriodic(1000, 5000, aLong -> {
            webclient.getAbs("https://real-time-finance-data.p.rapidapi.com/stock-quote")
                    .putHeader("x-rapidapi-host", "real-time-finance-data.p.rapidapi.com")
                    .putHeader("x-rapidapi-key", "1aba7153a3mshea7ffa54724c50ap1ea4dejsn02bff4cc3ef6")
                    .addQueryParam("symbol", "AAPL:NASDAQ")
                    .send()
                    .onSuccess(response -> {
                        logger.info(response.bodyAsString());
                    })
                    .onFailure(err -> logger.error("Error Observer"));
        });

        final long cleaner = vertx.setTimer(60 * 1000, aLong -> {
            logger.info("Canceling Scheduled Task");
            vertx.cancelTimer(taskId);
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            vertx.cancelTimer(cleaner);
            webclient.close();
            vertx.close();
        }));
    }
}