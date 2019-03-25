/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.util.Threads;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Transactional buffer is designed to register callbacks, to execute them when transaction commits and to clear them
 * when transaction rollbacks.
 */
@NotThreadSafe
public class TransactionalBuffer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalBuffer.class);

    private final Map<String, Transaction> transactions;
    private final ExecutorService executor;
    private final AtomicInteger taskCounter;
    private final ErrorHandler errorHandler;

    /**
     * Constructor to create a new instance.
     *
     * @param logicalName logical name
     * @param errorHandler error handler
     */
    TransactionalBuffer(String logicalName, ErrorHandler errorHandler) {
        this.transactions = new HashMap<>();
        this.executor = Threads.newSingleThreadExecutor(OracleConnector.class, logicalName, "transactional-buffer");
        this.taskCounter = new AtomicInteger();
        this.errorHandler = errorHandler;
    }

    /**
     * Registers callback to execute when transaction commits.
     *
     * @param transactionId transaction identifier
     * @param scn SCN
     * @param callback callback to execute when transaction commits
     */
    void registerCommitCallback(String transactionId, BigDecimal scn, CommitCallback callback) {
        transactions.computeIfAbsent(transactionId, s -> new Transaction(scn)).commitCallbacks.add(callback);
    }

    /**
     * Executes registered callbacks for given transaction identifier.
     *
     * @param transactionId transaction identifier
     * @param timestamp commit timestamp
     * @param context context to check that source is running
     */
    void commit(String transactionId, Timestamp timestamp, ChangeEventSource.ChangeEventSourceContext context) {
        Transaction transaction = transactions.remove(transactionId);
        if (transaction == null) {
            return;
        }
        LOGGER.trace("Transaction {} committed", transactionId);
        List<CommitCallback> commitCallbacks = transaction.commitCallbacks;
        BigDecimal smallestScn = transactions.isEmpty() ? null : calculateSmallestScn();
        taskCounter.incrementAndGet();
        executor.execute(() -> {
            try {
                for (CommitCallback callback : commitCallbacks) {
                    if (!context.isRunning()) {
                        return;
                    }
                    callback.execute(timestamp, smallestScn);
                }
            }
            catch (InterruptedException e) {
                LOGGER.error("Thread interrupted during running", e);
                Thread.currentThread().interrupt();
            }
            catch (Exception e) {
                errorHandler.setProducerThrowable(e);
            }
            finally {
                taskCounter.decrementAndGet();
            }
        });
    }

    private BigDecimal calculateSmallestScn() {
        return transactions.values()
                .stream()
                .map(transaction -> transaction.firstScn)
                .min(BigDecimal::compareTo)
                .orElseThrow(() -> new DataException("Cannot calculate smallest SCN"));
    }

    /**
     * Clears registered callbacks for given transaction identifier.
     *
     * @param transactionId transaction identifier
     */
    void rollback(String transactionId) {
        Transaction transaction = transactions.remove(transactionId);
        if (transaction != null) {
            LOGGER.trace("Transaction {} rolled back", transactionId);
        }
    }

    /**
     * Returns {@code true} if buffer is empty, otherwise {@code false}.
     *
     * @return {@code true} if buffer is empty, otherwise {@code false}
     */
    boolean isEmpty() {
        return transactions.isEmpty() && taskCounter.get() == 0;
    }

    /**
     * Closes buffer.
     */
    void close() {
        transactions.clear();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(1000L, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            LOGGER.error("Thread interrupted during shutdown", e);
        }
    }

    /**
     * Callback is designed to execute when transaction commits.
     */
    public interface CommitCallback {

        /**
         * Executes callback.
         *
         * @param timestamp commit timestamp
         * @param smallestScn smallest SCN among other transactions
         */
        void execute(Timestamp timestamp, BigDecimal smallestScn) throws InterruptedException;
    }

    @NotThreadSafe
    private static final class Transaction {

        private final BigDecimal firstScn;
        private final List<CommitCallback> commitCallbacks;

        private Transaction(BigDecimal firstScn) {
            this.firstScn = firstScn;
            this.commitCallbacks = new ArrayList<>();
        }
    }
}
