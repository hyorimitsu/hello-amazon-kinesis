package com.amazonaws.services.kinesis.samples.stocktrades.processor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;

import com.amazonaws.services.kinesis.samples.stocktrades.model.StockTrade;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

/**
 * ストリームから取得した株式取引のレコードを処理するクラスです。
 *
 */
public class StockTradeRecordProcessor implements ShardRecordProcessor {

    private static final Log log = LogFactory.getLog(StockTradeRecordProcessor.class);

    private String kinesisShardId;

    // レポート間隔
    private static final long REPORTING_INTERVAL_MILLIS = 60000L;
    private long nextReportingTimeInMillis;

    // チェックポイント(ストリームデータをどこまで処理したか)記録間隔
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
    private long nextCheckpointTimeInMillis;

    // 取引統計情報
    private StockStats stockStats = new StockStats();

    @Override
    public void initialize(InitializationInput initializationInput) {
        kinesisShardId = initializationInput.shardId();
        log.info("Initializing record processor for shard: " + kinesisShardId);
        log.info("Initializing @ Sequence: " + initializationInput.extendedSequenceNumber().toString());

        nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
        nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
         try {
            log.info("Processing " + processRecordsInput.records().size() + " record(s)");
            // 統計情報更新
            processRecordsInput.records().forEach(r -> processRecord(r));

            // レポートの表示とリセット
            if (System.currentTimeMillis() > nextReportingTimeInMillis) {
                reportStats();
                resetStats();
                nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
            }

            // チェックポイント記録
            if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
                checkpoint(processRecordsInput.checkpointer());
                nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
            }
        } catch (Throwable t) {
            log.error("Caught throwable while processing records. Aborting.");
            Runtime.getRuntime().halt(1);
        }

    }

    private void reportStats() {
        System.out.println("****** Shard " + kinesisShardId + " ******************************\n" +
                stockStats + "\n" +
                "****************************************************************\n");
    }

    private void resetStats() {
        stockStats = new StockStats();
    }

    private void processRecord(KinesisClientRecord record) {
        byte[] arr = new byte[record.data().remaining()];
        record.data().get(arr);
        StockTrade trade = StockTrade.fromJsonAsBytes(arr);
        if (trade == null) {
            log.warn("Skipping record. Unable to parse record into StockTrade. Partition Key: " + record.partitionKey());
            return;
        }
        stockStats.addStockTrade(trade);
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        log.info("Lost lease, so terminating.");
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        try {
            log.info("Reached shard end checkpointing.");
            shardEndedInput.checkpointer().checkpoint();
        } catch (ShutdownException | InvalidStateException e) {
            log.error("Exception while checkpointing at shard end. Giving up.", e);
        }
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        log.info("Scheduler is shutting down, checkpointing.");
        checkpoint(shutdownRequestedInput.checkpointer());

    }

    private void checkpoint(RecordProcessorCheckpointer checkpointer) {
        log.info("Checkpointing shard " + kinesisShardId);
        try {
            checkpointer.checkpoint();
        } catch (ShutdownException se) {
            log.info("Caught shutdown exception, skipping checkpoint.", se);
        } catch (ThrottlingException e) {
            log.error("Caught throttling exception, skipping checkpoint.", e);
        } catch (InvalidStateException e) {
            log.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
        }
    }

}
