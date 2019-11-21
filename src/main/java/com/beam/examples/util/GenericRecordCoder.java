package com.beam.examples.util;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.SchemaRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;

public class GenericRecordCoder extends AtomicCoder<GenericRecord> {

    public static GenericRecordCoder of() {
        return new GenericRecordCoder();
    }
    private static final ConcurrentHashMap<String, AvroCoder<GenericRecord>> avroCoders = new ConcurrentHashMap<>();
    @Override
    public void encode(GenericRecord value, OutputStream outStream) throws CoderException, IOException {
//        SchemaRegistry.registerIfAbsent(value.getSchema());
        String schemaName = value.getSchema().getFullName();
        StringUtf8Coder.of().encode(schemaName, outStream);
        AvroCoder<GenericRecord> coder = avroCoders.computeIfAbsent(schemaName,
                s -> AvroCoder.of(value.getSchema()));
        coder.encode(value, outStream);
    }

    @Override
    public GenericRecord decode(InputStream inStream) throws CoderException, IOException {
        return null;
    }
}
