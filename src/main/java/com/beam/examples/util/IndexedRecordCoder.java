package com.beam.examples.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class IndexedRecordCoder extends CustomCoder<GenericRecord> {

    @Override
    public void encode(GenericRecord value, OutputStream outStream) throws CoderException, IOException {

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream.reset();
        BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
        Schema schema = value.getSchema();
        GenericDatumWriter datumWriter = new GenericDatumWriter(schema);
        try {
            datumWriter.write(value, binaryEncoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            binaryEncoder.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        outStream.write(byteArrayOutputStream.toByteArray());
//        byte[] bytes = byteArrayOutputStream.toByteArray();


    }

    @Override
    public GenericRecord decode(InputStream inStream) throws CoderException, IOException {
        return null;
    }
}
