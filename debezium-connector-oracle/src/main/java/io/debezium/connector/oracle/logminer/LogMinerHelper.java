/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.math.BigInteger;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleDatabaseVersion;

/**
 * This class contains methods to configure and manage LogMiner utility
 */
public class LogMinerHelper {

    private static final String UNKNOWN = "unknown";
    private static final String TOTAL = "TOTAL";
    private static final String MAX_SCN_11_2 = "281474976710655";
    private static final String MAX_SCN_12_2 = "18446744073709551615";
    private static final String MAX_SCN_19_6 = "9295429630892703743";
    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerHelper.class);

    /**
     * Sets the log files to be used for mining.
     * This mimics the behavior when using the CONTINUOUS_MINE functionality of Oracle LogMiner.
     *
     * @param connection connection
     * @param lastProcessedScn current offset
     * @param archiveLogRetention the duration that archive logs will be mined
     * @throws SQLException if anything unexpected happens
     */
    // todo: check RAC resiliency
    public static void setRedoLogFilesForMining(OracleConnection connection, Long lastProcessedScn, Duration archiveLogRetention) throws SQLException {

        removeLogFilesFromMining(connection);

        Map<String, BigInteger> onlineLogFilesForMining = getOnlineLogFilesForOffsetScn(connection, lastProcessedScn);
        Map<String, BigInteger> archivedLogFilesForMining = getArchivedLogFilesForOffsetScn(connection, lastProcessedScn, archiveLogRetention);

        if (onlineLogFilesForMining.size() + archivedLogFilesForMining.size() == 0) {
            throw new IllegalStateException("None of log files contains offset SCN: " + lastProcessedScn + ", re-snapshot is required.");
        }

        List<String> logFilesNames = onlineLogFilesForMining.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());
        // remove duplications
        List<String> archivedLogFiles = archivedLogFilesForMining.entrySet().stream()
                .filter(e -> !onlineLogFilesForMining.values().contains(e.getValue())).map(Map.Entry::getKey).collect(Collectors.toList());
        logFilesNames.addAll(archivedLogFiles);

        for (String file : logFilesNames) {
            LOGGER.trace("Adding log file {} to mining session", file);
            String addLogFileStatement = SqlUtils.addLogFileStatement("DBMS_LOGMNR.ADDFILE", file);
            connection.prepareCall(false, addLogFileStatement);
        }

        LOGGER.debug("Last mined SCN: {}, Log file list to mine: {}\n", lastProcessedScn, logFilesNames);
    }

    /**
     * This method calculates SCN as a watermark to abandon long lasting transactions.
     * The criteria is don't let offset scn go out of archives older given number of hours
     *
     * @param connection connection
     * @param offsetScn current offset scn
     * @param transactionRetention duration to tolerate long running transactions
     * @return optional SCN as a watermark for abandonment
     */
    public static Optional<Long> getLastScnToAbandon(OracleConnection connection, Long offsetScn, Duration transactionRetention) {
        try {
            Float diffInDays = connection.querySingleResult(SqlUtils.diffInDaysQuery(offsetScn), (rs) -> rs.getFloat(1));
            if (diffInDays != null && (diffInDays * 24) > transactionRetention.toHours()) {
                return Optional.of(offsetScn);
            }
            return Optional.empty();
        }
        catch (SQLException e) {
            LOGGER.error("Cannot calculate days difference due to {}", e);
            return Optional.of(offsetScn);
        }
    }

    static void logWarn(TransactionalBufferMetrics metrics, String format, Object... args) {
        LOGGER.warn(format, args);
        metrics.incrementWarningCounter();
    }

    static void logError(TransactionalBufferMetrics metrics, String format, Object... args) {
        LOGGER.error(format, args);
        metrics.incrementErrorCounter();
    }

    /**
     * Get the size of the redo log groups.
     *
     * @param connection connection
     * @return number of redo log groups
     */
    private static int getRedoLogGroupSize(OracleConnection connection) throws SQLException {
        return getMap(connection, SqlUtils.allOnlineLogsQuery(), getDatabaseMaxScnValue(connection)).size();
    }

    /**
     * Get the database maximum SCN value
     *
     * @param connection the oracle database connection
     * @return the maximum scn value as a string value
     */
    public static String getDatabaseMaxScnValue(OracleConnection connection) {
        OracleDatabaseVersion version = connection.getOracleVersion();
        if ((version.getMajor() == 19 && version.getMaintenance() >= 6) || (version.getMajor() > 19)) {
            return MAX_SCN_19_6;
        }
        else if ((version.getMajor() == 12 && version.getMaintenance() >= 2) || (version.getMajor() > 12)) {
            return MAX_SCN_12_2;
        }
        else if ((version.getMajor() == 11 && version.getMaintenance() >= 2) || (version.getMajor() == 12 && version.getMaintenance() < 2)) {
            return MAX_SCN_11_2;
        }
        throw new RuntimeException("Max SCN cannot be resolved for database version " + version);
    }

    /**
     * Get all redo logs starting from the one which contains the offset SCN and ending with the one
     * that contains the largest SCN value.  The largest SCN value differs based on the Oracle
     * version.
     *
     * @param connection the database connection
     * @param offsetScn the offset scn
     * @return map of filename and next scn values for the given redo log file.
     * @throws SQLException if an exception occurs
     *
     * todo replace all longs using Scn
     */
    public static Map<String, BigInteger> getOnlineLogFilesForOffsetScn(OracleConnection connection, Long offsetScn) throws SQLException {
        // TODO: Make offset a BigInteger and refactor upstream
        BigInteger offsetScnBi = BigInteger.valueOf(offsetScn);
        LOGGER.trace("Getting online redo logs for offset scn {}", offsetScnBi);
        Map<String, ScnRange> redoLogFiles = new LinkedHashMap<>();

        String maxScnStr = getDatabaseMaxScnValue(connection);
        final BigInteger maxScn = new BigInteger(maxScnStr);
        connection.query(SqlUtils.allOnlineLogsQuery(), (rs) -> {
            while (rs.next()) {
                ScnRange range = new ScnRange(rs.getString(4), defaultIfNull(rs.getString(2), maxScnStr));
                redoLogFiles.put(rs.getString(1), range);
            }
        });
        return redoLogFiles.entrySet().stream()
                .filter(entry -> filterRedoLogEntry(entry, offsetScnBi, maxScn)).collect(Collectors
                        .toMap(Map.Entry::getKey, entry -> resolveNextChangeFromScnRange(entry.getValue().getNextChange(), maxScn)));
    }

    private static boolean filterRedoLogEntry(Map.Entry<String, ScnRange> entry, BigInteger offsetScn, BigInteger maxScn) {
        final BigInteger nextChangeNumber = new BigInteger(entry.getValue().getNextChange());
        if (nextChangeNumber.compareTo(offsetScn) >= 0 || nextChangeNumber.equals(maxScn)) {
            LOGGER.trace("Online redo log {} with SCN range {} to {} to be added.", entry.getKey(), entry.getValue().getFirstChange(), entry.getValue().getNextChange());
            return true;
        }
        LOGGER.trace("Online redo log {} with SCN range {} to {} to be excluded.", entry.getKey(), entry.getValue().getFirstChange(), entry.getValue().getNextChange());
        return false;
    }

    private static BigInteger resolveNextChangeFromScnRange(String nextChangeValue, BigInteger maxScn) {
        final BigInteger value = new BigInteger(nextChangeValue);
        return value.equals(maxScn) ? maxScn : value;
    }

    /**
     * Helper method that will dump the state of various critical tables used by the LogMiner implementation
     * to derive state about which logs are to be mined and processed by the Oracle LogMiner session.
     *
     * @param connection the database connection
     */
    public static void logDatabaseState(OracleConnection connection) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Configured redo logs are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$LOGFILE");
            }
            catch (SQLException e) {
                LOGGER.debug("Failed to obtain redo log table entries", e);
            }
            LOGGER.debug("Available archive logs are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$ARCHIVED_LOG");
            }
            catch (SQLException e) {
                LOGGER.debug("Failed to obtain archive log table entries", e);
            }
            LOGGER.debug("Available logs are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$LOG");
            }
            catch (SQLException e) {
                LOGGER.debug("Failed to obtain log table entries", e);
            }
            LOGGER.debug("Log history last 24 hours:");
            try {
                logQueryResults(connection, "SELECT * FROM V$LOG_HISTORY WHERE FIRST_TIME >= SYSDATE - 1");
            }
            catch (SQLException e) {
                LOGGER.debug("Failed to obtain log history", e);
            }
            LOGGER.debug("Log entries registered with LogMiner are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$LOGMNR_LOGS");
            }
            catch (SQLException e) {
                LOGGER.debug("Failed to obtain registered logs with LogMiner", e);
            }
            LOGGER.debug("Log mining session parameters are:");
            try {
                logQueryResults(connection, "SELECT * FROM V$LOGMNR_PARAMETERS");
            }
            catch (SQLException e) {
                LOGGER.debug("Failed to obtain log mining session parameters", e);
            }
        }
    }

    /**
     * Helper method that dumps the result set of an arbitrary SQL query to the connector's logs.
     *
     * @param connection the database connection
     * @param query the query to execute
     * @throws SQLException thrown if an exception occurs performing a SQL operation
     */
    private static void logQueryResults(OracleConnection connection, String query) throws SQLException {
        connection.query(query, rs -> {
            int columns = rs.getMetaData().getColumnCount();
            List<String> columnNames = new ArrayList<>();
            for (int index = 1; index <= columns; ++index) {
                columnNames.add(rs.getMetaData().getColumnName(index));
            }
            LOGGER.debug("{}", columnNames);
            while (rs.next()) {
                List<Object> columnValues = new ArrayList<>();
                for (int index = 1; index <= columns; ++index) {
                    columnValues.add(rs.getObject(index));
                }
                LOGGER.debug("{}", columnValues);
            }
        });
    }

    /**
     * This method returns all archived log files for one day, containing given offset scn
     * @param connection database connection
     * @param offsetScn offset scn
     * @param archiveLogRetention duration that archive logs will be mined
     * @return map of archived files
     * @throws SQLException if an exception occurs
     */
    public static Map<String, BigInteger> getArchivedLogFilesForOffsetScn(OracleConnection connection, Long offsetScn, Duration archiveLogRetention) throws SQLException {
        final Map<String, String> redoLogFiles = new LinkedHashMap<>();
        final String maxScnStr = getDatabaseMaxScnValue(connection);
        final BigInteger maxScn = new BigInteger(maxScnStr);

        final String query = SqlUtils.archiveLogsQuery(offsetScn, archiveLogRetention);
        connection.query(query, (rs) -> {
            while (rs.next()) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Archive log {} with SCN range {} to {} to be added.", rs.getString(1), rs.getString(3), rs.getString(2));
                }
                redoLogFiles.put(rs.getString(2), defaultIfNull(rs.getString(2), maxScnStr));
            }
        });
        return redoLogFiles.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> resolveNextChangeFromScnRange(e.getValue(), maxScn)));
    }

    /**
     * This method removes all added log files from mining
     *
     * @param conn the database connection
     * @throws SQLException a database exception occurred
     */
    public static void removeLogFilesFromMining(OracleConnection conn) throws SQLException {
        conn.query("SELECT FILENAME AS NAME FROM V$LOGMNR_LOGS", (rs) -> {
            while (rs.next()) {
                String fileName = rs.getString(1);
                conn.prepareCallWithCommit(false, SqlUtils.deleteLogFileStatement(fileName));
                LOGGER.debug("File {} was removed from mining", fileName);
            }
        });
    }

    @Deprecated
    public static Map<String, String> getMap(OracleConnection connection, String query, String nullReplacement) throws SQLException {
        Map<String, String> result = new LinkedHashMap<>();
        connection.query(query, (rs) -> {
            while (rs.next()) {
                String value = rs.getString(2);
                result.put(rs.getString(1), value == null ? nullReplacement : value);
            }
        });
        return result;
    }

    @Deprecated
    private static String defaultIfNull(String value, String replacement) {
        return value != null ? value : replacement;
    }

    /**
     * Holds the first and next change number range for a log entry.
     */
    private static class ScnRange {
        private final String firstChange;
        private final String nextChange;

        public ScnRange(String firstChange, String nextChange) {
            this.firstChange = firstChange;
            this.nextChange = nextChange;
        }

        public String getFirstChange() {
            return firstChange;
        }

        public String getNextChange() {
            return nextChange;
        }
    }
}
