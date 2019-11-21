package com.beam.examples.util;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static com.beam.examples.DataflowPipeline.logger;
import static com.beam.examples.constants.Constants.TABLE_NAME;


public class QueryGenerator {

    public static final List<Schema.Type> schemaType = new ArrayList() {
    };

    public String generateQueryFromSchema(Schema schema, Properties properties) {
        String tableName = properties.getProperty(TABLE_NAME);
        String queryString = "SELECT ";
        try {
            List<Schema.Field> schemaFields = schema.getFields();
            for (Schema.Field field : schemaFields) {
                String fieldName = field.name();
                String queryColumn = properties.getProperty(fieldName);
                queryString = queryString + queryColumn + ",";
            }

            queryString = queryString.substring(0, queryString.lastIndexOf(",")).trim();
            queryString = queryString + " FROM " + properties.getProperty("SQL_TABLE_NAME");
            logger.info("[QueryGenerator] - Successfully generated Jdbc Query from Schema as {}", queryString);
        } catch (Exception e) {
            logger.error("[QueryGenerator] - Error while Generating Jdbc Query from Consumer Schema, Exception : {}", e.getMessage());
            e.printStackTrace();
        }


        return queryString;
    }



};
