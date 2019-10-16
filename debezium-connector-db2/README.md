# Running in Kafka Connect

Using the Confluent distribution:

Put the jars in CONFLUENT_DIR/share/java:

debezium-connector-db2-1.0.0-SNAPSHOT.jar  
debezium-core-1.0.0-SNAPSHOT.jar  
jcc-11.5.0.0.jar

Set the CLASSPATH accordingly:

export CLASSPATH=CONFLUENT_DIR/share/java/debezium-connector-db2-1.0.0-SNAPSHOT.jar:CONFLUENT_DIR/share/java/debezium-core-1.0.0-SNAPSHOT.jar:CONFLUENT_DIR/share/java/jcc-11.5.0.0.jar:$CLASSPATH

CONFLUENT_DIR/bin/connect-standalone CONFLUENT_DIR/etc/kafka/connect-standalone.properties db2-connector.properties

Where the properties file db2-connector.properties is:

name=db2-connector
connector.class=io.debezium.connector.db2.Db2Connector
database.hostname=localhost
database.port=50000
database.user=db2inst1
database.password=admin
database.server.name=TESTDB
database.dbname=TESTDB
database.cdcschema=ASNCDC
database.history.kafka.bootstrap.servers=localhost:9092
database.history.kafka.topic=CDCTESTDB
database.history.use.catalog.before.schema=false