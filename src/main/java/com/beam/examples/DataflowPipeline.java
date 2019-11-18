package com.beam.examples;

import com.beam.examples.credentials.CredentialsManager;
import com.beam.examples.dao.DataAccessor;
import com.beam.examples.util.SchemaProvider;
import com.google.auth.oauth2.GoogleCredentials;
import org.apache.avro.Schema;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static com.beam.examples.constants.Constants.*;

public class DataflowPipeline {

    public static void main(String[] args) throws Exception {
        GoogleCredentials credentials = CredentialsManager.loadGoogleCredentials(GCP_API_KEY);
        DataAccessor dao = new DataAccessor();

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

        String kafkaTopic = properties.getProperty("KAFKA_TOPIC");
        String bootStrapServer = properties.getProperty("KAFKA_BOOTSTRAP_SERVER");

        PCollection<KafkaRecord<String, String>> kafkaData = dao.readFromKafkaTopic(pipeline, bootStrapServer, kafkaTopic);

        String kafkaSchemaRegistry = properties.getProperty(SCHEMA_REGISTRY_URL);

        PCollectionView<String> view = pipeline.apply(Create.of(kafkaSchemaRegistry)).apply(View.asSingleton());

        PCollection<Schema> schemaList = kafkaData.apply(ParDo.of(new DoFn<KafkaRecord<String, String>, Schema>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {
                KafkaRecord<String, String> record = processContext.element();
                String kafkaSchemaRegistry = processContext.sideInput(view);
                String message = record.getKV().getValue();
                SchemaProvider schemaProvider = new SchemaProvider();
                Schema schema = SchemaProvider.readConsumerSchema(kafkaSchemaRegistry);
                processContext.output(schema);
            }


        }).withSideInputs(view));

        schemaList.apply(ParDo.of(new DoFn<Schema, String>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {
                String schemaString = processContext.element().toString();
                System.out.println(schemaString);
            }
        }));

        pipeline.run().waitUntilFinish();
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
