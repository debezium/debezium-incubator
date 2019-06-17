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
            String sql = res.getString(6);
            Long segmentType = res.getLong(7);
            Long status = res.getLong(8);
            Long operationCode = res.getLong(9);
            String tableName = res.getString(10);
            Timestamp changeTime = res.getTimestamp(11);
            Timestamp commitTime = res.getTimestamp(12);
            byte[] txId = res.getBytes(13);
            Long txSequenceNumber = res.getLong(14);

            boolean tableParsed = false;
            for  (TableId tableId : schema.getTables().tableIds()) {
                if (tableId.table().toLowerCase().contains(tableName.toLowerCase())){
                    tableParsed = true;
                    break;
                }
            }

            if(!tableParsed){
                LOGGER.debug("table " + tableName + " is not in the whitelist, skipped");
                continue;
            }

            //TCL
            if (operationCode == 7 || operationCode == 36) {
                LOGGER.info("TCL, actualScn= " + actualScn + " - " + operation + " - " + sql);
                // todo
            }

            // DDL
            if (operationCode == 5) {
                LOGGER.info("DDL, actualScn= " + actualScn + " - " + operation + " - " + sql);
                // todo parse, add to the collection.
            }

            // DML (insert,delete,update,commit,rollback)
            if (operationCode == 1 || operationCode == 2 || operationCode == 3) {
                LOGGER.info("DML, actualScn= " + actualScn + " - " + operation + " - " + sql);
                try {

                    dmlParser.parse(sql, schema.getTables());
                    LogMinerRowLcr rowLcr = dmlParser.getDmlChange();
                    if (rowLcr == null) {
                        LOGGER.error("Following statement cannot be parsed: " + sql);
                        continue;
                    }
                    rowLcr.setObjectOwner(userName);
                    rowLcr.setSourceTime(commitTime);
                    rowLcr.setSourceDatabaseName(pdbName);
                    String transactionId = DatatypeConverter.printHexBinary(txId);
                    rowLcr.setTransactionId(transactionId);
                    rowLcr.setObjectName(tableName);

                    rowLcr.setActualCommitScn(actualCommitScn);
                    rowLcr.setActualScn(actualScn);
                    LOGGER.debug("rowLcr:  username ="+ userName+",commit time="+commitTime+
                            ", trID="+transactionId+",actualCommitScn="+actualCommitScn+", actualScn="+actualScn);

                    changes.add(rowLcr);

                } catch (Exception e) {
                    LOGGER.error("Following statement: " + sql + " cannot be parsed due to the : " + e);
                }
            }
        }
        return changes;
    }
}
