package com.beam.examples.util;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class GenericRecorderializer extends AbstractKafkaAvroSerializer implements Serializer<GenericRecord> {


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        configure(new KafkaAvroSerializerConfig(configs));
    }

    //    @Override
    public byte[] serialize(String topic, List<GenericRecord> data) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream.reset();
        BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);

        for (GenericRecord record : data) {
            Schema schema = record.getSchema();
            GenericDatumWriter datumWriter = new GenericDatumWriter(schema);
            try {
                datumWriter.write(record, binaryEncoder);
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                binaryEncoder.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        byte[] bytes = byteArrayOutputStream.toByteArray();
        return bytes;
    }


    @Override
    public byte[] serialize(String topic, GenericRecord data) {
        Schema schema = data.getSchema();
        SpecificDatumWriter<GenericRecord>
                datumWriter =
                new SpecificDatumWriter<>(schema);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream.reset();
        BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
        try {
            datumWriter.write(data, binaryEncoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            binaryEncoder.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] bytes = byteArrayOutputStream.toByteArray();
        return bytes;
    }

    @Override
    public void close() {

    }
}
