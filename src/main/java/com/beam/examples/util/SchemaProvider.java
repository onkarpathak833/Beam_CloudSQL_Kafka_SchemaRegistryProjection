package com.beam.examples.util;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;

public class SchemaProvider {

    public static Schema readConsumerSchema(String kafkaSchemaRegistry) {

//        String kafkaSchemaRegistry = properties.getProperty(Constants.SCHEMA_REGISTRY_URL);

        SchemaRegistryClient client = new CachedSchemaRegistryClient(kafkaSchemaRegistry, 100);
        Schema schema = null;
        try {
            SchemaMetadata metadata = client.getSchemaMetadata("order-value" +
                    "", 1);
            schema = Schema.parse(metadata.getSchema());
            System.out.println(metadata.getSchema());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }

        return schema;
    }

    public static class SchemaGenerator extends DoFn<KafkaRecord<String, String>, Schema> {

    }

}
