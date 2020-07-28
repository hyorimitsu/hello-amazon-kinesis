package com.amazonaws.services.kinesis.samples.stocktrades.processor;

import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

/**
 * 新しい株式取引のプロセッサを作成するクラスです。
 *
 */
public class StockTradeRecordProcessorFactory implements ShardRecordProcessorFactory {
    @Override
    public ShardRecordProcessor shardRecordProcessor() {
        return new StockTradeRecordProcessor();
    }

}
