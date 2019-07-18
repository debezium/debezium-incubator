/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.jdbc.JdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * This class contains methods to configure and manage Log Miner utility
 */
public class LogMinerHelper {

    private final static Logger LOGGER = LoggerFactory.getLogger(LogMinerHelper.class);

    /**
     * This builds data dictionary objects in redo log files. This is the first step of the LogMiner configuring.
     *
     * @param connection connection to the database as log miner user (connection to the container)
     * @throws SQLException fatal exception, cannot continue further
     */
    public static void buildDataDictionary(Connection connection) throws SQLException {
        executeCallableStatement(connection, SqlUtils.BUILD_DICTIONARY);
    }

    /**
     * This method returns current SCN from the database
     *
     * @param connection container level database connection
     * @return current SCN
     * @throws SQLException fatal exception, cannot continue further
     */
    public static long getCurrentScn(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(SqlUtils.CURRENT_SCN)) {

            if (!rs.next()) {
                throw new IllegalStateException("Couldn't get SCN");
            }

            long currentScn = rs.getLong(1);
            rs.close();
            return currentScn;

        }
    }

    /**
     * This adds all online redo log files for mining.
     *
     * @param connection container level database connection
     * @throws SQLException fatal exception, cannot continue further
     */
    public static void addOnlineRedoLogFilesForMining(Connection connection) throws SQLException {
        List<String> fileNames = getOnlineRedoLogFiles(connection);
        if (fileNames.size() < 1) {
            throw new RuntimeException("Cannot fetch redo log files info");
        }

        String option;
        int i = 0;
        for (String fileName : fileNames) {
            if (i == 0) {
                option = "DBMS_LOGMNR.NEW";
                i = 1;
            } else {
                option = "DBMS_LOGMNR.ADDFILE";
            }
            String addLogFileStatement = SqlUtils.getAddLogFileStatement(option, fileName);
            LOGGER.debug("log file = {}, option: {} ", fileName, option);
            executeCallableStatement(connection, addLogFileStatement);
        }
    }

    /**
     * This method builds mining view to query changes from.
     * This view is built for online redo log files.
     * It starts log mining session.
     * It uses data dictionary objects, incorporated in previous steps.
     * It tracks DDL changes and mines committed data only.
     *
     * @param connection container level database connection
     * @param startScn   the SCN to mine from
     * @param endScn     the SCN to mine to
     * @throws SQLException fatal exception, cannot continue further
     */
    public static void startOnlineMining(Connection connection, Long startScn, Long endScn) throws SQLException {
        String statement = SqlUtils.getStartLogMinerStatement(startScn, endScn);
        executeCallableStatement(connection, statement);
        // todo dbms_logmnr.STRING_LITERALS_IN_STMT?
        // todo If the log file is corrupter bad, logmnr will not be able to access it, what to do?
    }

    /**
     * This method query the database to get CURRENT online redo log file
     * @param connection connection to reuse
     * @return full redo log file name, including path
     * @throws SQLException this would be something fatal
     */
    public static String getCurrentRedoLogFile(Connection connection) throws SQLException {
        String checkQuery = "select f.member " +
                "from v$log log, v$logfile f  where log.group#=f.group# and log.status='CURRENT'";

        String fileName = "";
        PreparedStatement st = connection.prepareStatement(checkQuery);
        ResultSet result = st.executeQuery();
        while (result.next()) {
            fileName = result.getString(1);
            LOGGER.trace(" Current Redo log fileName: {} ",  fileName);
        }
        st.close();
        result.close();

        return fileName;
    }

    /**
     * This method fetches the oldest SCN from online redo log files
     *
     * @param connection container level database connection
     * @return oldest SCN from online redo log
     * @throws SQLException fatal exception, cannot continue further
     */
    public static long getFirstOnlineLogScn(Connection connection) throws SQLException {
        LOGGER.debug("getting first scn of online log");
        Statement s = connection.createStatement();
        ResultSet res = s.executeQuery(SqlUtils.OLDEST_FIRST_CHANGE);
        res.next();
        long firstScnOfOnlineLog = res.getLong(1);
        res.close();
        return firstScnOfOnlineLog;
    }

    /**
     * This method is fetching changes from a batch of archived log files
     *
     * @param conn                 container level database connection
     * @param batchSize            number of archived files to mine at once
     * @param schemaName           user that made schema changes
     * @param archivedFileIterator Iterator for archived files
     * @return ResultSet result
     * @throws SQLException fatal exception, cannot continue further
     */
    public static ResultSet getArchivedChanges(Connection conn, int batchSize, String schemaName, Iterator<Map.Entry<String, Long>> archivedFileIterator) throws SQLException {

        while (batchSize > 0 && archivedFileIterator.hasNext()) {
            String name = archivedFileIterator.next().getKey();
            addRedoLogFileForMining(conn, name);
            LOGGER.debug("{} was added for mining", archivedFileIterator);
            batchSize--;
        }

        buildArchivedMiningView(conn);

        // mining
        PreparedStatement getChangesQuery = conn.prepareStatement(SqlUtils.queryLogMinerArchivedContents(schemaName));
        ResultSet result = getChangesQuery.executeQuery();
        getChangesQuery.close();
        return result;
    }

    /**
     * This method fetches all archived log files names, starting from the one which contains offset SCN.
     * This is needed what the connector is down for a long time and some changes were archived.
     * This will help to catch up old changes during downtime.
     *
     * @param offsetScn  offset SCN
     * @param connection container level database connection
     * @return java.util.Map as List of archived log filenames and it's next_change# field value
     * @throws SQLException fatal exception, cannot continue further
     */
    public static Map<String, Long> getArchivedLogFiles(long offsetScn, Connection connection) throws SQLException {
        // this happens for the first connection start. No offset yet, we should capture current changes only.
        if (offsetScn == -1) {
            return Collections.emptyMap();
        }

        long oldestArchivedScn = getFirstArchivedLogScn(connection);
        if (offsetScn < oldestArchivedScn) {
            throw new SQLException("There are no log files containing this SCN. " +
                    "Most likely the connector was down for a long time and archived log files were purged");
        }

        Map<String, Long> allLogs = new TreeMap<>();

        PreparedStatement ps = connection.prepareStatement(SqlUtils.LATEST_SCN_FROM_ARCHIVED_LOG);
        ResultSet res = ps.executeQuery();
        res.next();
        long maxArchivedScn = res.getLong(1);

        ps = connection.prepareStatement(SqlUtils.ALL_ARCHIVED_LOGS_NAMES_FOR_OFFSET);
        ps.setLong(1, offsetScn);
        ps.setLong(2, maxArchivedScn);
        res = ps.executeQuery();
        while (res.next()) {
            allLogs.put(res.getString(1), res.getLong(2));
            LOGGER.info("Log file to mine: {}, next change = {} ", res.getString(1),res.getLong(2));
        }
        ps.close();
        res.close();
        return allLogs;
    }

    /**
     * After a switch, we should remove it from the analysis.
     * NOTE. It does not physically remove the log file.
     *
     * @param logFileName file to delete from the analysis
     * @param connection  container level database connection
     * @throws SQLException fatal exception, cannot continue further
     */
    public static void removeLogFileFromMining(String logFileName, Connection connection) throws SQLException {
        String removeLogFileFromMining = SqlUtils.getRemoveLogFileFromMiningStatement(logFileName);
        executeCallableStatement(connection, removeLogFileFromMining);
        LOGGER.debug("{} was removed from mining", removeLogFileFromMining);

    }

    /**
     * Sets NLS parameters for mining session.
     *
     * @param connection container level database connection
     * @throws SQLException if anything unexpected happens
     */
    public static void setNlsSessionParameters(JdbcConnection connection) throws SQLException {
        connection.executeWithoutCommitting(SqlUtils.NLS_SESSION_PARAMETERS);
    }

    /**
     * This call completes log miner session.
     * Complete gracefully.
     *
     * @param connection container level database connection
     */
    static void endMining(Connection connection) {
        String stopMining = SqlUtils.END_LOGMNR;
        try {
            executeCallableStatement(connection, stopMining);
        } catch (SQLException e) {
            LOGGER.error("Cannot end mining session properly due to the:{} ", e.getMessage());
        }
    }

    /**
     * This method fetches the oldest SCN from online redo log files
     *
     * @param connection container level database connection
     * @return oldest SCN from online redo log
     * @throws SQLException fatal exception, cannot continue further
     */
    private static long getFirstArchivedLogScn(Connection connection) throws SQLException {
        LOGGER.debug("getting first scn of archived log");
        Statement s = connection.createStatement();
        ResultSet res = s.executeQuery(SqlUtils.OLDEST_ARCHIVED_CHANGE);
        res.next();
        long firstScnOfOnlineLog = res.getLong(1);
        s.close();
        res.close();
        return firstScnOfOnlineLog;
    }

    /**
     * A method to add log file for mining
     *
     * @param connection Database connection
     * @param fileName   Redo log file name
     * @throws SQLException fatal exception
     */
    public static void addRedoLogFileForMining(Connection connection, String fileName) throws SQLException {
        final String addLogFileStatement = SqlUtils.getAddLogFileStatement("DBMS_LOGMNR.ADDFILE", fileName);
        executeCallableStatement(connection, addLogFileStatement);
        LOGGER.debug("Redo log file= {} added for mining", fileName);
    }


    /**
     * This method builds mining view to query changes from.
     * This view is built for archived log files.
     * It uses data dictionary from online catalog. However DDL will be available.
     *
     * @param connection container level database connection
     * @throws SQLException fatal exception, cannot continue further
     */
    private static void buildArchivedMiningView(Connection connection) throws SQLException {
        executeCallableStatement(connection, SqlUtils.START_LOGMINER_FOR_ARCHIVE_STATEMENT);
        // todo If the archive is bad, logMiner will not be able to access it, maybe we should continue gracefully:
    }

    /**
     * This method returns online redo log file names which are in ACTIVE, INACTIVE or CURRENT states
     * UNUSED will be ignored.
     * This is because UNUSED files cannot be added for mining
     *
     * @param connection container level database connection
     * @return List of redo log file names
     * @throws SQLException fatal exception, cannot continue further
     */
    private static List<String> getOnlineRedoLogFiles(Connection connection) throws SQLException {
        List<String> fileNames = new ArrayList<>();
        PreparedStatement statement = connection.prepareStatement(SqlUtils.ONLINE_LOG_FILENAME);
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            fileNames.add(rs.getString(1));
        }
        rs.close();
        return fileNames;
    }

    private static void executeCallableStatement(Connection connection, String statement) throws SQLException {
        Objects.requireNonNull(statement);
        CallableStatement s;
        s = connection.prepareCall(statement);
        s.execute();
        s.close();
    }

}
