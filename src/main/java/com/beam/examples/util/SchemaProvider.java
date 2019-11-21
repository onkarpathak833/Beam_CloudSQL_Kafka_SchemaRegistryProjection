package com.beam.examples.util;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.beam.examples.DataflowPipeline.logger;

enum Type {
    RECORD, ARRAY, MAP, UNION, FIXED, STRING, BYTES, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL;
    private final String name;

    private Type() {
        this.name = this.name().toLowerCase(Locale.ENGLISH);
    }

    public String getName() {
        return name;
    }
}

public class SchemaProvider {

    public static Schema readConsumerSchema(String kafkaSchemaRegistry) {

//        String kafkaSchemaRegistry = properties.getProperty(Constants.SCHEMA_REGISTRY_URL);

        SchemaRegistryClient client = new CachedSchemaRegistryClient(kafkaSchemaRegistry, 100);
        Schema schema = null;
        Schema typeSchema = null;
        try {
            SchemaMetadata metadata = client.getSchemaMetadata("order-value", 1);
            String schemaString = metadata.getSchema();
            int schemaID = metadata.getId();
            int schemaVersion = metadata.getVersion();
            logger.info("[SchemaProvider] - Successfully read consumer schema " +
                    "from Registry. Schema {},  Version {}, Schema ID {}", schemaString, schemaID, schemaVersion);

            schema = new Schema.Parser().parse(metadata.getSchema());
            logger.info("[SchemaProvider] - Parsed Consumer schema as Avro Schema Type Schema Parsed is : {}", schema);

            List<Schema.Field> generatedSchemaFields = new ArrayList<>();
            List<Schema.Field> schemaFields = schema.getFields();


            typeSchema = Schema.createRecord("ConsumerSchema", "this is consumer schema", "record", false);
            for (Schema.Field field : schemaFields) {

                if (checkIfPrimitive(field.schema().getType())) {
                    Schema.Field typeField = new Schema.Field(field.name(), field.schema(), null, null);
                    generatedSchemaFields.add(typeField);
                } else {
                    Schema.Field typeField = new Schema.Field(field.name(), Schema.create(Schema.Type.INT), null, null);
                    generatedSchemaFields.add(typeField);
                }

            }

            typeSchema.setFields(generatedSchemaFields);

            logger.info("[SchemaProvider] - Generated Schema from input schema is {}. \n This schema will be used for Generic Record creation.", typeSchema);

        } catch (IOException e) {
            logger.error("[SchemaProvider] - Error while reading consumer schema from Registry. Exception Mesage {}", e.getMessage());
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }

        return typeSchema;
    }

    private static boolean checkIfPrimitive(Schema.Type type) {
        for (Type type1 : Type.values()) {
            if (type1.getName().equals(type.getName()) && !type.getName().equals(Type.RECORD.getName())) {
                return true;
            }
        }
        return false;
    }

    public byte[] createGenericRecord(String tableSchema, ResultSet resultSet, Map<String, String> properties) {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            Schema schema = new Schema.Parser().parse(tableSchema);

            List<Schema.Field> fields = schema.getFields();
            GenericDatumWriter writer = new GenericDatumWriter<GenericRecord>(schema);
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
            logger.info("[SchemaProvider] - Generating Avro Record for SQL Resultset");
            while (resultSet.next()) {
                GenericRecord record = new GenericData.Record(schema);
                for (Schema.Field field : fields) {
                    String fieldName = field.name();
                    logger.info(" Schema Type :--> {}", schema.getType().getName());
                    logger.info("Field Name :--> " + fieldName + " Type :--> " + field.schema().getName());
                    String jdbcColumnName = properties.get(fieldName);
                    Object value = null;
                    try {
                        value = resultSet.getObject(jdbcColumnName);
                        if (value instanceof BigDecimal) {
                            value = Double.valueOf(value.toString());
                        }
                        record.put(fieldName, value);
                    } catch (Exception e) {
                        logger.error("[SchemaProvider] - Error while Traversing Schema fields for GenericRecord put. Exception : {}", e.getMessage());
                        e.printStackTrace();
                    }
                }

                logger.info("[SchemaProvider] - Generated Avro Generic record {}", record);
                writer.write((Object) record, encoder);
                encoder.flush();

            }
            encoder.flush();
            logger.info("[SchemaProvider] - Successully Built Avro GenericRecord for Resultset");

        } catch (Exception e) {
            logger.error("[SchemaProvider] - Error while generating Avro Generic Record for Resultset. Exception {}", e.getMessage());
            e.printStackTrace();
        }
        logger.info("[SchemaProvider] - Generated Byte Array for Resultset. Byte Array Size : {}", baos.size());
        return baos.toByteArray();
    }


}

