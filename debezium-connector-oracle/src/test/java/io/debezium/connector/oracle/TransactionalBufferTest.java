/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_BATCH_SIZE;
import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE;
import static org.fest.assertions.Assertions.assertThat;

public class TransactionalBufferTest {

    private static final String SERVER_NAME = "serverX";
    private static final String TRANSACTION_ID = "transaction";
    private static final String OTHER_TRANSACTION_ID = "other_transaction";
    private static final BigDecimal SCN = BigDecimal.ONE;
    private static final BigDecimal OTHER_SCN = BigDecimal.TEN;
    private static final Timestamp TIMESTAMP = new Timestamp(System.currentTimeMillis());

    private ErrorHandler errorHandler;
    private TransactionalBuffer transactionalBuffer;

    @Before
    public void before() {
        ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(Duration.of(DEFAULT_MAX_QUEUE_SIZE, ChronoUnit.MILLIS))
                .maxBatchSize(DEFAULT_MAX_BATCH_SIZE)
                .maxQueueSize(DEFAULT_MAX_QUEUE_SIZE)
                .build();
        errorHandler = new ErrorHandler(OracleConnector.class, SERVER_NAME, queue, () -> { });
        transactionalBuffer = new TransactionalBuffer(SERVER_NAME, errorHandler);
    }

    @After
    public void after() throws InterruptedException {
        errorHandler.stop();
        transactionalBuffer.close();
    }

    @Test
    public void testIsEmpty() {
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(true);
    }

    @Test
    public void testIsNotEmptyWhenTransactionIsRegistered() {
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, (timestamp, smallestScn) -> { });
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(false);
    }

    @Test
    public void testIsNotEmptyWhenTransactionIsCommitting() {
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, (timestamp, smallestScn) -> Thread.sleep(1000));
        transactionalBuffer.commit(TRANSACTION_ID, TIMESTAMP, () -> true);
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(false);
    }

    @Test
    public void testIsEmptyWhenTransactionIsCommitted() throws InterruptedException {
        CountDownLatch commitLatch = new CountDownLatch(1);
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, (timestamp, smallestScn) -> commitLatch.countDown());
        transactionalBuffer.commit(TRANSACTION_ID, TIMESTAMP, () -> true);
        commitLatch.await();
        Thread.sleep(1000);
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(true);
    }

    @Test
    public void testIsEmptyWhenTransactionIsRolledBack() {
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, (timestamp, smallestScn) -> { });
        transactionalBuffer.rollback(TRANSACTION_ID);
        assertThat(transactionalBuffer.isEmpty()).isEqualTo(true);
    }

    @Test
    public void testCalculateSmallestScnWhenTransactionIsCommitted() throws InterruptedException {
        CountDownLatch commitLatch = new CountDownLatch(1);
        AtomicReference<BigDecimal> smallestScnContainer = new AtomicReference<>();
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, (timestamp, smallestScn) -> {
            smallestScnContainer.set(smallestScn);
            commitLatch.countDown();
        });
        transactionalBuffer.commit(TRANSACTION_ID, TIMESTAMP, () -> true);
        commitLatch.await();
        assertThat(smallestScnContainer.get()).isNull();
    }

    @Test
    public void testCalculateSmallestScnWhenFirstTransactionIsCommitted() throws InterruptedException {
        CountDownLatch commitLatch = new CountDownLatch(1);
        AtomicReference<BigDecimal> smallestScnContainer = new AtomicReference<>();
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, (timestamp, smallestScn) -> {
            smallestScnContainer.set(smallestScn);
            commitLatch.countDown();
        });
        transactionalBuffer.registerCommitCallback(OTHER_TRANSACTION_ID, OTHER_SCN, (timestamp, smallestScn) -> { });
        transactionalBuffer.commit(TRANSACTION_ID, TIMESTAMP, () -> true);
        commitLatch.await();
        assertThat(smallestScnContainer.get()).isEqualTo(OTHER_SCN);
    }

    @Test
    public void testCalculateSmallestScnWhenSecondTransactionIsCommitted() throws InterruptedException {
        transactionalBuffer.registerCommitCallback(TRANSACTION_ID, SCN, (timestamp, smallestScn) -> { });
        CountDownLatch commitLatch = new CountDownLatch(1);
        AtomicReference<BigDecimal> smallestScnContainer = new AtomicReference<>();
        transactionalBuffer.registerCommitCallback(OTHER_TRANSACTION_ID, OTHER_SCN, (timestamp, smallestScn) -> {
            smallestScnContainer.set(smallestScn);
            commitLatch.countDown();
        });
        transactionalBuffer.commit(OTHER_TRANSACTION_ID, TIMESTAMP, () -> true);
        commitLatch.await();
        assertThat(smallestScnContainer.get()).isEqualTo(SCN);
    }
}
