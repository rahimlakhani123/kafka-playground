package org.example.price.source;

import java.util.List;

public interface PriceFetcher {
    void fetchPrice(List<String> symbols);
    void stop();
}
