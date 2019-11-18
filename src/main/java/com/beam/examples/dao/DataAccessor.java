package com.beam.examples.dao;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Properties;

public class DataAccessor {

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

    public PCollection<KafkaRecord<String, String>> readFromKafkaTopic(Pipeline pipeline, String bootStrapServer, String topic) {

        System.out.println(bootStrapServer);
        System.out.println(topic);
        PCollection<KafkaRecord<String, String>> kafkaObjects = pipeline.apply(KafkaIO.<String, String>read()
                .withTopic(topic)
                .withBootstrapServers(bootStrapServer)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
        );
        return kafkaObjects;
    }

}
