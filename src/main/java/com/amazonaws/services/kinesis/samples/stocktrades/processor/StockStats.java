package com.amazonaws.services.kinesis.samples.stocktrades.processor;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.kinesis.samples.stocktrades.model.StockTrade;
import com.amazonaws.services.kinesis.samples.stocktrades.model.StockTrade.TradeType;

/**
 * 株式取引の統計情報を管理するクラスです。
 */
public class StockStats {

    // 取引タイプごとの各銘柄の取引数
    private EnumMap<TradeType, Map<String, Long>> countsByTradeType;

    // 取引タイプごとのもっとも人気のある銘柄
    private EnumMap<TradeType, String> mostPopularByTradeType;

    public StockStats() {
        countsByTradeType = new EnumMap<TradeType, Map<String, Long>>(TradeType.class);
        for (TradeType tradeType: TradeType.values()) {
            countsByTradeType.put(tradeType, new HashMap<String, Long>());
        }

        mostPopularByTradeType = new EnumMap<TradeType, String>(TradeType.class);
    }

    /**
     * 取引情報を受け取り、統計を更新します。
     *
     * @param trade 株式取引
     */
    public void addStockTrade(StockTrade trade) {
        // 取引数を更新
        TradeType type = trade.getTradeType();
        Map<String, Long> counts = countsByTradeType.get(type);
        Long count = counts.get(trade.getTickerSymbol());
        if (count == null) {
            count = 0L;
        }
        counts.put(trade.getTickerSymbol(), ++count);

        // 人気銘柄を更新
        String mostPopular = mostPopularByTradeType.get(type);
        if (mostPopular == null || countsByTradeType.get(type).get(mostPopular) < count) {
            mostPopularByTradeType.put(type, trade.getTickerSymbol());
        }
    }

    public String toString() {
        return String.format(
                "人気銘柄（買い）: %s, %d %n" +
                "人気銘柄（売り）: %s, %d ",
                getMostPopularStock(TradeType.BUY), getMostPopularStockCount(TradeType.BUY),
                getMostPopularStock(TradeType.SELL), getMostPopularStockCount(TradeType.SELL));
    }

    private String getMostPopularStock(TradeType tradeType) {
        return mostPopularByTradeType.get(tradeType);
    }

    private Long getMostPopularStockCount(TradeType tradeType) {
        String mostPopular = getMostPopularStock(tradeType);
        return countsByTradeType.get(tradeType).get(mostPopular);
    }
}
