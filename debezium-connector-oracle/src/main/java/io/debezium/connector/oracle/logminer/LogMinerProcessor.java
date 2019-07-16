/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.antlr.OracleDmlParser;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerRowLcr;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * This class processes ResultSet from LogMiner view by parsing it and converting
 * into List of value holders LogMinerRowLcr
 *
 */
public class LogMinerProcessor {
    private final OracleDatabaseSchema schema;
    private final OracleDmlParser dmlParser;
    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerProcessor.class);

    public LogMinerProcessor(OracleDatabaseSchema schema, OracleDmlParser dmlParser) {
        this.schema = schema;
        this.dmlParser = dmlParser;
    }

    public List<LogMinerRowLcr> process (ResultSet res) throws SQLException {

        List<LogMinerRowLcr> changes = new ArrayList<>();
        while(res.next()) {
            long actualScn = res.getLong(1);
            long actualCommitScn = res.getLong(2);
            String operation = res.getString(3);
            String userName = res.getString(4);
            String pdbName = res.getString(5);
            String redo_sql = res.getString(6);
            StringBuilder sqlBuilder = new StringBuilder(redo_sql);
            Long segmentType = res.getLong(7);
            Long status = res.getLong(8);
            Long operationCode = res.getLong(9);
            String tableName = res.getString(10);
            Timestamp changeTime = res.getTimestamp(11);
            Timestamp commitTime = res.getTimestamp(12);
            byte[] txId = res.getBytes(13);
            Long txSequenceNumber = res.getLong(14);
            int csf = res.getInt(15);
            String segOwner = res.getString(16);
            String segName = res.getString(17);
            int sequence = res.getInt(18);
            int lobLimit = 40000;// todo : we can make it configurable or use chunk approach, similar to XStream
            while (csf == 1) {
                res.next();
                if (lobLimit-- == 0) {
                    LOGGER.warn("LOB value for SCN= {} was truncated due to the connector limitation of {} MB", actualScn, 4*lobLimit/1000  );
                    break;
                }
                csf = res.getInt(15);
                sqlBuilder.append(res.getString(6));
            }
            String sql = sqlBuilder.toString();

            // Commit
            if (operationCode == 7) {
                LOGGER.info("COMMIT, transactionId = {}, actualScn= {}, committed SCN= {}, userName= {},segOwner={}, segName={}, sequence={}",
                        txId, actualScn,actualCommitScn,userName,segOwner,segName,sequence);
                continue;
            }

            boolean tableParsed = false;
            for  (TableId tableId : schema.getTables().tableIds()) {
                if (tableName!= null && tableId.table().toLowerCase().contains(tableName.toLowerCase())){
                    tableParsed = true;
                    break;
                }
            }

            if(!tableParsed){
                LOGGER.debug("table {} is not in the whitelist, skipped", tableName);
                continue;
            }

            // DDL
            if (operationCode == 5) {
                LOGGER.info("DDL, transactionId = {}, actualScn= {}, committed SCN= {}, userName= {},segOwner={}, segName={}, sequence={}",
                        txId, actualScn,actualCommitScn,userName,segOwner,segName,sequence);
                // todo parse, add to the collection.
            }


            // DML (insert,delete,update,commit,rollback)
            if (operationCode == 1 || operationCode == 2 || operationCode == 3) {
                LOGGER.info("DML, transactionId = {}, actualScn= {}, committed SCN= {}, operation: {}, SQL: {}, userName= {}, segOwner={}, segName={}, sequence={}",
                        txId, actualScn,actualCommitScn,operation,sql,userName,segOwner,segName,sequence);
                try {

                    dmlParser.parse(sql, schema.getTables());
                    LogMinerRowLcr rowLcr = dmlParser.getDmlChange();
                    if (rowLcr == null) {
                        LOGGER.error("Following statement was not parsed: {}", sql);
                        continue;
                    }
                    String transactionId = DatatypeConverter.printHexBinary(txId);
                    LOGGER.debug("rowLcr:  username = {},change time={}, trID={},actualCommitScn={}, actualScn={}", userName, changeTime, transactionId, actualCommitScn, actualScn);
                    rowLcr.setObjectOwner(userName);
                    rowLcr.setSourceTime(changeTime);
                    rowLcr.setSourceDatabaseName(pdbName);
                    rowLcr.setTransactionId(transactionId);
                    rowLcr.setObjectName(tableName);

                    rowLcr.setActualCommitScn(actualCommitScn);
                    rowLcr.setActualScn(actualScn);

                    changes.add(rowLcr);

                } catch (Exception e) {
                    LOGGER.error("Following statement: {} cannot be parsed due to the : {}",sql, e.getMessage());
                }
            }
        }
        return changes;
    }
}
