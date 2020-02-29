package org.embulk.output.bigquery_java;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public interface BigqueryRetryable<T> {
    Logger logger = LoggerFactory.getLogger(BigqueryRetryable.class.getCanonicalName());
    int MAX_TRIES = 5;
    // https://cloud.google.com/bigquery/quotas
    // Maximum rate of table metadata update operations — 5 operations every 10 seconds per table
    int BIGQUERY_TABLE_OPERATION_INTERVAL = 2;

    default int getMaxTries(){
        return MAX_TRIES;
    }

    default T executeWithRetry(int count, Supplier<T> func) throws InterruptedException {
        if(count >= getMaxTries()) {
            logger.info("Retry out");
            throw new RuntimeException("Max retry limit exceed");
        }
        try {
            return func.get();
        } catch (Exception e){
            logger.info("Retrying... {} times", count);
            Thread.sleep(BIGQUERY_TABLE_OPERATION_INTERVAL * 1000);
            return executeWithRetry(count + 1, func);
        }
    }
}
