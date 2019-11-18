package com.beam.examples.util;

import org.apache.avro.Schema;

import java.util.List;
import java.util.Properties;

import static com.beam.examples.constants.Constants.TABLE_NAME;

public class QueryGenerator {

    public String generateSchema(Schema schema, Properties properties) {
        String tableName = properties.getProperty(TABLE_NAME);
        List<Schema.Field> schemaFields = schema.getFields();
        String queryString = "SELECT ";
        for(Schema.Field field : schemaFields) {
            String fieldName = field.name();
            String queryColumn = properties.getProperty(fieldName);
            queryString = queryString + queryColumn+",";
        }

        queryString = queryString.substring(0, queryString.lastIndexOf(",")).trim();
        queryString = queryString + " FROM "+properties.getProperty("SQL_TABLE_NAME");
        return queryString;
    }
}
