package com.amazonaws.services.kinesis.samples.stocktrades.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.services.kinesis.samples.stocktrades.model.StockTrade;

/**
 * 株式取引情報を生成するクラスです。
 */
public class StockTradeGenerator {

    private static final List<StockPrice> STOCK_PRICES = new ArrayList<StockPrice>();
    static {
        STOCK_PRICES.add(new StockPrice("AAPL", 119.72));
        STOCK_PRICES.add(new StockPrice("XOM", 91.56));
        STOCK_PRICES.add(new StockPrice("GOOG", 527.83));
        STOCK_PRICES.add(new StockPrice("BRK.A", 223999.88));
        STOCK_PRICES.add(new StockPrice("MSFT", 42.36));
        STOCK_PRICES.add(new StockPrice("WFC", 54.21));
        STOCK_PRICES.add(new StockPrice("JNJ", 99.78));
        STOCK_PRICES.add(new StockPrice("WMT", 85.91));
        STOCK_PRICES.add(new StockPrice("CHL", 66.96));
        STOCK_PRICES.add(new StockPrice("GE", 24.64));
        STOCK_PRICES.add(new StockPrice("NVS", 102.46));
        STOCK_PRICES.add(new StockPrice("PG", 85.05));
        STOCK_PRICES.add(new StockPrice("JPM", 57.82));
        STOCK_PRICES.add(new StockPrice("RDS.A", 66.72));
        STOCK_PRICES.add(new StockPrice("CVX", 110.43));
        STOCK_PRICES.add(new StockPrice("PFE", 33.07));
        STOCK_PRICES.add(new StockPrice("FB", 74.44));
        STOCK_PRICES.add(new StockPrice("VZ", 49.09));
        STOCK_PRICES.add(new StockPrice("PTR", 111.08));
        STOCK_PRICES.add(new StockPrice("BUD", 120.39));
        STOCK_PRICES.add(new StockPrice("ORCL", 43.40));
        STOCK_PRICES.add(new StockPrice("KO", 41.23));
        STOCK_PRICES.add(new StockPrice("T", 34.64));
        STOCK_PRICES.add(new StockPrice("DIS", 101.73));
        STOCK_PRICES.add(new StockPrice("AMZN", 370.56));
    }

    private static final double MAX_DEVIATION = 0.2;

    private static final int MAX_QUANTITY = 10000;

    private static final double PROBABILITY_SELL = 0.4;

    private final Random random = new Random();
    private AtomicLong id = new AtomicLong(1);

    public StockTrade getRandomTrade() {
        StockPrice stockPrice = STOCK_PRICES.get(random.nextInt(STOCK_PRICES.size()));
        double deviation = (random.nextDouble() - 0.5) * 2.0 * MAX_DEVIATION;
        double price = stockPrice.price * (1 + deviation);
        price = Math.round(price * 100.0) / 100.0;

        StockTrade.TradeType tradeType = StockTrade.TradeType.BUY;
        if (random.nextDouble() < PROBABILITY_SELL) {
            tradeType = StockTrade.TradeType.SELL;
        }

        long quantity = random.nextInt(MAX_QUANTITY) + 1;

        return new StockTrade(stockPrice.tickerSymbol, tradeType, price, quantity, id.getAndIncrement());
    }

    private static class StockPrice {
        String tickerSymbol;
        double price;

        StockPrice(String tickerSymbol, double price) {
            this.tickerSymbol = tickerSymbol;
            this.price = price;
        }
    }

}
