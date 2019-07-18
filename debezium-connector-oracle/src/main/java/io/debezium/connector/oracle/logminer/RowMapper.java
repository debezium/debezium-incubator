/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * A utility class to map LogMiner content resultset values
 */
public class RowMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(RowMapper.class);

    //operations
    public static final int INSERT = 1;
    public static final int DELETE = 2;
    public static final int UPDATE = 3;
    public static final int DDL = 5;
    public static final int COMMIT = 7;
    public static final int ROLLBACK = 36;

    private static final int SCN = 1;
    private static final int COMMIT_SCN = 2;
    private static final int OPERATION = 3;
    private static final int USER_NAME = 4;
    private static final int PDB_NAME = 5;  //containerized DB
    private static final int SQL_REDO = 6;
    private static final int OPERATION_CODE = 9;
    private static final int TABLE_NAME = 10;
    private static final int CHANGE_TIME = 11;
    private static final int TX_ID = 13;
    private static final int CSF = 15;
    private static final int SEG_OWNER = 16;
    private static final int SEG_NAME = 17;
    private static final int SEQUENCE = 18;


    public static int getOperationCode(ResultSet rs) throws SQLException {
        return rs.getInt(OPERATION_CODE);
    }

    public static String getOperation(ResultSet rs) throws SQLException {
        return rs.getString(OPERATION);
    }

    public static String getUserName(ResultSet rs) throws SQLException {
        return rs.getString(USER_NAME);
    }

    public static String getTableName(ResultSet rs) throws SQLException {
        return rs.getString(TABLE_NAME);
    }

    public static String getSegOwner(ResultSet rs) throws SQLException {
        return rs.getString(SEG_OWNER);
    }

    public static String getSegName(ResultSet rs) throws SQLException {
        return rs.getString(SEG_NAME);
    }

    public static int getSequence(ResultSet rs) throws SQLException {
        return rs.getInt(SEQUENCE);
    }

    public static Timestamp getChangeTime(ResultSet rs) throws SQLException {
        return rs.getTimestamp(CHANGE_TIME);
    }

    public static BigDecimal getScn(ResultSet rs) throws SQLException {
        return rs.getBigDecimal(SCN);
    }

    public static BigDecimal getCommitScn(ResultSet rs) throws SQLException {
        return rs.getBigDecimal(COMMIT_SCN);
    }

    public static String getTransactionId(ResultSet rs) throws SQLException {
        return DatatypeConverter.printHexBinary(rs.getBytes(TX_ID));
    }

    public static String getSqlRedo(ResultSet rs) throws SQLException {
        int csf = rs.getInt(CSF);
        // 0 - indicates SQL_REDO is contained within the same row
        // 1 - indicates that either SQL_REDO is greater than 4000 bytes in size and is continued in
        // the next row returned by the ResultSet
        if (csf == 0) {
            return rs.getString(SQL_REDO);
        } else {
            final StringBuilder result = new StringBuilder(rs.getString(SQL_REDO));
            int lobLimit = 10000;// todo : decide on approach ( XStream chunk option) and Lob limit
            BigDecimal scn = getScn(rs);
            while (csf == 1) {
                rs.next();
                if (lobLimit-- == 0) {
                    LOGGER.warn("LOB value for SCN= {} was truncated due to the connector limitation of {} MB", scn, 40);
                    break;
                }
                csf = rs.getInt(CSF);
                result.append(rs.getString(SQL_REDO));
            }
            return result.toString();
        }
    }

    public static TableId getTableId(String catalogName, ResultSet rs) throws SQLException {
        return new TableId(catalogName.toUpperCase(), rs.getString(SEG_OWNER).toUpperCase(), rs.getString(TABLE_NAME).toUpperCase());
    }

}
