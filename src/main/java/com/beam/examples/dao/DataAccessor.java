package com.beam.examples.dao;

import com.beam.examples.constants.Constants;
import com.beam.examples.util.SchemaProvider;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

import static com.beam.examples.DataflowPipeline.logger;
import static com.beam.examples.constants.Constants.SCHEMA_REGISTRY_URL;
import static com.beam.examples.constants.Constants.kafka_bootstrap_server;

public class DataAccessor {
    private static JdbcIO.DataSourceConfiguration config = null;
    private static String postgresConnectionURL = null;
    private static SchemaProvider schemaProvider = null;
    private static Connection connection = null;
    private static Statement statement = null;
    private static KafkaProducer<Long, GenericRecord> kafkaProducer = null;

    static {
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

    private static KafkaProducer<Long, GenericRecord> createKafkaProducerFor(Map properties) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.get(kafka_bootstrap_server).toString());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicNameStrategy.class.getName());
        props.put(KafkaAvroSerializerConfig.KEY_SUBJECT_NAME_STRATEGY, TopicNameStrategy.class.getName());
        // Schema Registry location.
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, properties.get(SCHEMA_REGISTRY_URL));
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, "true");
//        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS_DEFAULT, "true");
        props.put(SchemaRegistryConfig.COMPATIBILITY_CONFIG, "none");
        return new KafkaProducer<Long, GenericRecord>(props);
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
        } catch (Exception e) {
            logger.error("[DataAccessor] - Error while Fetching Resultset or converting Avro Generic Record.");
            e.printStackTrace();
        }
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

    public void publishToKafkaTopic(Map properties, List<GenericRecord> avroRecords) {
        if (kafkaProducer == null) {
            kafkaProducer = createKafkaProducerFor(properties);
        }
        String kafkaTopic = (String) properties.get(Constants.TARGET_KAFKA_TOPIC);
        avroRecords.stream().forEach(record -> {
            logger.info("[DataflowPipeline] - Sending Avro Generic Record to Target Kafka Topic.");
            long randomKey = new Random().nextLong();
            ProducerRecord<Long, GenericRecord> producerRecord = new ProducerRecord(kafkaTopic, randomKey, record);
            kafkaProducer.send(producerRecord);
        });
        kafkaProducer.flush();
    }

}
