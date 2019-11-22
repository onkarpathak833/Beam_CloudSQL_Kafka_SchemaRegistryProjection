package com.beam.examples;

import com.beam.examples.credentials.CredentialsManager;
import com.beam.examples.dao.DataAccessor;
import com.beam.examples.util.QueryGenerator;
import com.beam.examples.util.SchemaProvider;
import com.google.auth.oauth2.GoogleCredentials;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.ResultSet;
import java.util.*;

import static com.beam.examples.constants.Constants.*;

public class DataflowPipeline {
    public static Logger logger = null;
    private static QueryGenerator queryGenerator = null;
    private static DataAccessor dao = null;
    private static SchemaProvider schemaProvider = null;

    static {
        queryGenerator = new QueryGenerator();
        dao = new DataAccessor();
        schemaProvider = new SchemaProvider();
        logger = LoggerFactory.getLogger("BeamJobLogger");
    }

    public static void main(String[] args) throws Exception {
        GoogleCredentials credentials = CredentialsManager.loadGoogleCredentials(GCP_API_KEY);
        Pipeline pipeline = createDataflowPipeline(args);
        MyPipelineOptions.MyCustomOptions options = (MyPipelineOptions.MyCustomOptions) pipeline.getOptions();
        String filePath = options.getConfigFilePath();
        File applicationConfigPath = new File(filePath);
        Properties properties = new Properties();
        InputStream ins = new FileInputStream(applicationConfigPath);
        try {
            properties.load(ins);
        } catch (Exception e) {
            System.out.println("Error while reading config file");
            e.printStackTrace();
        }


        String kafkaTopic = properties.getProperty(kafka_topic);
        String bootStrapServer = properties.getProperty(kafka_bootstrap_server);
        PCollection<KafkaRecord<String, String>> kafkaData = dao.readFromKafkaTopic(pipeline, bootStrapServer, kafkaTopic);

        String kafkaSchemaRegistry = properties.getProperty(SCHEMA_REGISTRY_URL);

        logger.info("[DataflowPipeline] - Kafka Schema Registry URL is {}", kafkaSchemaRegistry);
        Schema globalSchema = SchemaProvider.readConsumerSchema(kafkaSchemaRegistry);
        PCollectionView<String> view = pipeline.apply(Create.of(kafkaSchemaRegistry)).apply(View.asSingleton());

        logger.info("[DataflowPipeline] - Created Schema registry URL as side input view.");
        PCollection<Schema> schemaList = kafkaData.apply(ParDo.of(new DoFn<KafkaRecord<String, String>, Schema>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {
                KafkaRecord<String, String> record = processContext.element();
                String kafkaSchemaRegistry = processContext.sideInput(view);
                String message = record.getKV().getValue();
                logger.info("[DataflowPipeline] - Kafka Message Consumed with Key {}", record.getKV().getKey());
                logger.info("[DataflowPipeline] - Processing Kafka Records in ParDo for Schema registry");
                Schema schema = SchemaProvider.readConsumerSchema(kafkaSchemaRegistry);
                logger.info("[DataflowPipeline] - Return Schema Registry output to Process Context Output {}", schema);
                processContext.output(schema);
            }
        }).withSideInputs(view));

        Map<String, String> propertiesMap = getMapFromProperties(properties);

        PCollectionView<Map<String, String>> propertiesCollection = pipeline.apply(Create.of(propertiesMap)).apply(View.asMap());

        PCollection<List<String>> queryCollection = schemaList.apply(ParDo.of(new DoFn<Schema, List<String>>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {
                Schema schema = processContext.element();
                Map<String, String> data = processContext.sideInput(propertiesCollection);
                logger.info("[DataflowPipeline] - Generating Jdbc Query from Consumer Schema {}", schema);
                String query = queryGenerator.generateQueryFromSchema(schema, properties);
                System.out.println("Query Generated is : " + query);
                List list = new ArrayList<String>();
                list.add(0, query);
                list.add(1, schema.toString());
                logger.info("[DataflowPipeline] - Sending Query and Schema as Process Context Output");
                processContext.output(list);
            }
        }).withSideInputs(propertiesCollection));

        queryCollection.apply(ParDo.of(new DoFn<List<String>, List<GenericRecord>>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {
                List<String> inputs = processContext.element();
                Map<String, String> properties = processContext.sideInput(propertiesCollection);
                String kafkaTopic = properties.get("TARGET_KAFKA_TOPIC");
                String tableSchema = inputs.get(1);
                logger.info("[DataflowPipeline] - Processing Jdbc Query and Schema in ParDo");
                ResultSet resultSet = dao.readDataFromPostgreSQL(inputs.get(0));
                List<GenericRecord> avroRecords = schemaProvider.createGenericRecord(tableSchema, resultSet, properties);
                System.out.println("Generated Avro Record : " + avroRecords.size());
                dao.publishToKafkaTopic(properties, avroRecords);
            }
        }).withSideInputs(propertiesCollection)).setCoder(ListCoder.of(AvroCoder.of(globalSchema)));

        logger.info("[DataflowPipeline] - Write Avro Generic Records to Kafka Topic");
        pipeline.run().waitUntilFinish();
    }

    @NotNull
    private static Map<String, String> getMapFromProperties(Properties properties) {
        Map<String, String> propertiesMap = new HashMap<>();
        properties.keySet().stream().forEach(key -> {
            propertiesMap.put(key.toString(), properties.getProperty(key.toString()));
        });
        return propertiesMap;
    }


    private static Pipeline createDataflowPipeline(String[] args) {

        PipelineOptionsFactory.register(MyPipelineOptions.MyCustomOptions.class);
        MyPipelineOptions.MyCustomOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .create()
                .as(MyPipelineOptions.MyCustomOptions.class);

        options.setRunner(DirectRunner.class);
        options.setProject(PROJECT_ID);
        logger.info("[DataflowPipeline] - Setting Project as {}", PROJECT_ID);
        return Pipeline.create(options);
    }


    public static class MyPipelineOptions {

        public interface MyCustomOptions extends DataflowPipelineOptions {

            @Description("Configuration file for pipeline")
            @Default.String("/resources/application.config")
            String getConfigFilePath();

            void setConfigFilePath(String configFilePath);
        }

    }


}
