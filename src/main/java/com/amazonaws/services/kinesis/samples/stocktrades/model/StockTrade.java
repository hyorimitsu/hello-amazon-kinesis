package com.amazonaws.services.kinesis.samples.stocktrades.model;

import java.io.IOException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 株式取引クラスです。
 */
public class StockTrade {

    private final static ObjectMapper JSON = new ObjectMapper();

    static {
        JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public enum TradeType {
        BUY,
        SELL
    }

    /**
     * 銘柄
     */
    private String tickerSymbol;
    /**
     * 取引タイプ（売り/買い）
     */
    private TradeType tradeType;
    /**
     * 株価
     */
    private double price;
    /**
     * 株数
     */
    private long quantity;
    /**
     * ID
     */
    private long id;

    public StockTrade() {
    }

    public StockTrade(String tickerSymbol, TradeType tradeType, double price, long quantity, long id) {
        this.tickerSymbol = tickerSymbol;
        this.tradeType = tradeType;
        this.price = price;
        this.quantity = quantity;
        this.id = id;
    }

    public String getTickerSymbol() {
        return tickerSymbol;
    }

    public TradeType getTradeType() {
        return tradeType;
    }

    public double getPrice() {
        return price;
    }

    public long getQuantity() {
        return quantity;
    }

    public long getId() {
        return id;
    }

    public byte[] toJsonAsBytes() {
        try {
            return JSON.writeValueAsBytes(this);
        } catch (IOException e) {
            return null;
        }
    }

    public static StockTrade fromJsonAsBytes(byte[] bytes) {
        try {
            return JSON.readValue(bytes, StockTrade.class);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return String.format("ID %d: %s %d shares of %s for $%.02f",
                id, tradeType, quantity, tickerSymbol, price);
    }

}
