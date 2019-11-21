package com.beam.examples.dao;

import com.beam.examples.util.SchemaProvider;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.beam.examples.DataflowPipeline.logger;

public class DataAccessor {
    private static JdbcIO.DataSourceConfiguration config = null;
    private static String postgresConnectionURL = null;
    private static SchemaProvider schemaProvider = null;
    private static Connection connection = null;
    private static Statement statement = null;

    static {
//        config = JdbcIO.DataSourceConfiguration
//                .create("com.mysql.jdbc.Driver", "jdbc:mysql://google/test?cloudSqlInstance=project1-186407:us-east1:example-mysql-db&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=onkar&password=1234!@#$&useUnicode=true&characterEncoding=UTF-8");

        config = JdbcIO.DataSourceConfiguration
                .create("org.postgresql.Driver", "jdbc:postgresql://google/test?cloudSqlInstance=project1-186407:us-east1:example-db&socketFactory=com.google.cloud.sql.postgres.SocketFactory&user=onkar&password=1234!@#$&useUnicode=true&characterEncoding=UTF-8");

        postgresConnectionURL = "jdbc:postgresql://google/test?cloudSqlInstance=project1-186407:us-east1:example-db&socketFactory=com.google.cloud.sql.postgres.SocketFactory&user=onkar&password=1234!@#$&useUnicode=true&characterEncoding=UTF-8";
        schemaProvider = new SchemaProvider();

        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(postgresConnectionURL);
            statement = connection.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void readCloudSQLData(Properties properties) {

        String str = "{\n" +
                "    \"keys\": [\"emp_id\", \"emp_name\"],\n" +
                "    \"values\": [101, \"ABCD\"],\n" +
                "    \"target\": [\"employee\"]\n" +
                "}";
        JSONObject object = new JSONObject(str);

        JSONArray arr = object.getJSONArray("target");

        arr.get(0).toString();

    }

    public ResultSet readDataFromPostgreSQL(String query) {

        ResultSet resultSet = null;
        try {
            resultSet = statement.executeQuery(query);
            logger.info("[DataAccessor] - Fetched Resultset for Jdbc Query");
//            record = schemaProvider.createGenericRecord(schemaString, resultSet);
        } catch (Exception e) {
            logger.error("[DataAccessor] - Error while Fetching Resultset or converting Avro Generic Record.");
            e.printStackTrace();
        }

//        JdbcIO.Read<GenericRecord> data = JdbcIO.<GenericRecord>read().withDataSourceConfiguration(config)
//                .withQuery(query)
//                .withCoder(AvroCoder.of(new Schema.Parser().parse(schemaString)))
//                .withRowMapper((JdbcIO.RowMapper<GenericRecord>) resultSet -> {
//                    GenericRecord record = schemaProvider.createGenericRecord(schemaString, resultSet);
//                    return record;
//                });
//
        return resultSet;
    }

    public PCollection<KafkaRecord<String, String>> readFromKafkaTopic(Pipeline pipeline, String bootStrapServer, String topic) {

        System.out.println(bootStrapServer);
        System.out.println(topic);
        System.out.println(" Reading Data from " + topic + " of server : " + bootStrapServer);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "producer");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        Map<String, Object> propertiesMap = props.entrySet().stream().collect(
                Collectors.toMap(
                        e -> e.getKey().toString(),
                        e -> e.getValue()
                )
        );

        logger.info("[DataAccessor] - [Read Data From Kafka Topic] - Reading Messages from Kafka Topic {} and Bootstrap Serevr {}", topic, bootStrapServer);
        PCollection<KafkaRecord<String, String>> kafkaObjects = pipeline.apply(KafkaIO.<String, String>read()
                .withTopic(topic)
                .withBootstrapServers(bootStrapServer)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withConsumerConfigUpdates(propertiesMap)
        );

        logger.info("[DataAccessor] - [Read Data From Kafka Topic] - Messages consumed succesfully from Kafka Topic.");
        return kafkaObjects;
    }

}
