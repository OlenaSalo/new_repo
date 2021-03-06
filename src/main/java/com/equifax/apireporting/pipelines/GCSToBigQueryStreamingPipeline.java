package com.equifax.apireporting.pipelines;

import com.equifax.apireporting.pipelines.commons.SchemaParser;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.io.FilenameUtils;
import org.joda.time.Duration;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A template that copies data from a GCS bucket to an existing BigQuery table.
 */
public class GCSToBigQueryStreamingPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(GCSToBigQueryStreamingPipeline.class);

    private static final String BIGQUERY_SCHEMA = "BigQuery Schema";
    private static final String NAME = "name";
    private static final String TYPE = "type";
    private static final String MODE = "mode";
    private static final String FIELDS = "fields";
    private static final String TYPE_RECORD = "RECORD";
    private static final String TYPE_STRUCT = "STRUCT";
    private static final int MAX_NESTING = 15;

    private static final String MARKER = "DATA_UPLOAD_SKIPPED";

    /** Options supported by {@link GCSToBigQueryStreamingPipeline}. */
    public interface Options extends PipelineOptions, StreamingOptions {
        //  New options for streaming pipeline.
        //  Active here means data, schema and table, set for the actual data upload to the Google BigQuery table.
        @Validation.Required
        @Description("Temporary directory for BigQuery loading process")
        ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

        void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);

        @Description("The Cloud Pub/Sub topic to read from" + "for example: projects/PROJECT_ID/topics/TOPIC_NAME")
        @Validation.Required
        ValueProvider<String> getInputTopic();

        void setInputTopic(ValueProvider<String> value);

        @Description("GCS path to the active data set file.")
        ValueProvider<String> getActiveDataSetFilePath();

        void setActiveDataSetFilePath(ValueProvider<String> value);

        @Description("GCS path to the active schema file.")
        ValueProvider<String> getActiveSchemaFilePath();

        void setActiveSchemaFilePath(ValueProvider<String> value);

        @Description("Active target BigQuery table name.")
        ValueProvider<String> getActiveBiqQueryTableName();

        void setActiveBiqQueryTableName(ValueProvider<String> value);

        @Description("Active file name path.")
        ValueProvider<String> getActiveDataSetFileNameWithPath();

        void setActiveDataSetFileNameWithPath(ValueProvider<String> value);

        @Description("Google BigQuery ProjectID")
        ValueProvider<String> getProjectID();

        void setProjectID(ValueProvider<String> value);

        @Description("Google CS bucket name to watch for file(s) changes.")
        @Validation.Required
        ValueProvider<String> getBucketName();

        void setBucketName(ValueProvider<String> value);

        @Description("Schema folder sub path, without bucket name.")
        ValueProvider<String> getSchemaFolderPath();

        void setSchemaFolderPath(ValueProvider<String> value);

        @Description("Data folder sub path, without bucket name.")
        ValueProvider<String> getDataFolderPath();

        void setDataFolderPath(ValueProvider<String> value);

        @Description("Google BigQuery data set name.")
        ValueProvider<String> getBqDataSetName();

        void setBqDataSetName(ValueProvider<String> value);

        @Description("GCS data set name." + "for example: /some_folder/some_subfolder/data.json")
        ValueProvider<String> getDataFileName();

        void setDataFileName(ValueProvider<String> value);

        @Description("Data file extension")
        @Default.String("json")
        String getDataFileExtension();

        void setDataFileExtension(String value);

        @Description("Output file's window size in number of minutes.")
        @Default.Integer(60)
        Integer getWindowSize();

        void setWindowSize(Integer value);
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        options.setStreaming(true);

        pipeline
                .apply("Read message from Google Pub/Sub",
                        PubsubIO.readStrings().fromTopic(options.getInputTopic()))
                .apply("Window input from Google Pub/Sub into into finite windows with windowSize interval",
                        Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))))
                .apply("Make decision on dataset to be uploaded to Google BigQuery",
                        ParDo.of(new FlowConfigurator()))
                .apply("Read from Google GCS",
                        ParDo.of(new DataLoader()))
                .apply("Convert JSON file to TableRow object",
                        jsonToTableRow())
                .apply("Insert data into Google BigQuery table",
                        BigQueryIO.writeTableRows()
                                .withSchema(
                                        ValueProvider.NestedValueProvider.of(
                                                options.getActiveSchemaFilePath(),
                                                new SerializableFunction<String, TableSchema>() {

                                                    @Override
                                                    public TableSchema apply(String jsonPath) {

                                                        TableSchema tableSchema = new TableSchema();
                                                        List<TableFieldSchema> fields = new ArrayList<>();
                                                        SchemaParser schemaParser = new SchemaParser();
                                                        JSONObject jsonSchema;

                                                        try {
                                                            jsonSchema = schemaParser.parseSchema(jsonPath);

                                                            JSONArray bqSchemaJsonArray =
                                                                    jsonSchema.getJSONArray(BIGQUERY_SCHEMA);

                                                            tableSchema.setFields(extractFields(bqSchemaJsonArray, 0));
                                                        } catch (Exception e) {
                                                            throw new RuntimeException(e);
                                                        }

                                                        LOG.info("[INFO] JSON schema parsing is successful.");

                                                        return tableSchema;
                                                    }

                                                    private List<TableFieldSchema> extractFields(JSONArray bqSchemaJsonArray, int level) {
                                                        List<TableFieldSchema> fields = new ArrayList<>();
                                                        if (level > MAX_NESTING) {
                                                            return fields;
                                                        }

                                                        for (int i = 0; i < bqSchemaJsonArray.length(); i++) {
                                                            JSONObject inputField = bqSchemaJsonArray.getJSONObject(i);
                                                            TableFieldSchema field =
                                                                    new TableFieldSchema()
                                                                            .setName(inputField.getString(NAME))
                                                                            .setType(inputField.getString(TYPE));
                                                            if (inputField.has(MODE)) {
                                                                field.setMode(inputField.getString(MODE));
                                                                if (inputField.getString(TYPE).equalsIgnoreCase(TYPE_RECORD) ||
                                                                        inputField.getString(TYPE).equalsIgnoreCase(TYPE_STRUCT)) {
                                                                    if (inputField.has(FIELDS)) {
                                                                        field.setFields(extractFields(inputField.getJSONArray(FIELDS), level + 1));
                                                                    }
                                                                }
                                                            }
                                                            fields.add(field);
                                                        }

                                                        return fields;
                                                    }
                                                }))
                                .to(options.getActiveBiqQueryTableName())
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory()));

        pipeline.run().waitUntilFinish();
    }

    /** Make pipeline's options configuration, depends on comparative parameter(s) */
    public static class FlowConfigurator extends DoFn<String, String> {
        @ProcessElement
        public void processElement (ProcessContext processContext) {
            JSONObject arrivedMessage = new JSONObject(processContext.element());

            LOG.info("[INFO] GCSToBigQueryStreamingPipeline received message from Google Pub/Sub : " + arrivedMessage);

            //  Get parameters for triggered bucket and file names from Google Pub/Sub message
            String triggeredDataFileName = arrivedMessage.getString("name");
            String triggeredBucketName = arrivedMessage.getString("bucket");
            String triggeredDataFileExtension = getOnlyFileExtension(triggeredDataFileName);

            //  Get observed data file name from pipeline start options.
            Options options = processContext.getPipelineOptions().as(Options.class);

            String observedDataFolderPath = options.getDataFolderPath().toString();
            String observedBucketName = options.getBucketName().toString();
            String observedDataFileExtension = options.getDataFileExtension();

            //  Check that the files names are the same and make decision on workflow.
            if (checkForCoincidenceTest(triggeredDataFileName,
                    triggeredBucketName,
                    triggeredDataFileExtension,
                    observedDataFolderPath,
                    observedBucketName,
                    observedDataFileExtension)) {
                LOG.info("[INFO] Starting data upload to Google BigQuery...");

                //  Set pipeline options parameters for the active workflow.
                options.setActiveDataSetFileNameWithPath(ValueProvider.StaticValueProvider.of(triggeredDataFileName));
                options.setActiveDataSetFilePath(ValueProvider.StaticValueProvider.of("gs://" +
                        triggeredBucketName + options.getDataFolderPath().toString() + getFileNameWithExtension(triggeredDataFileName)));
                options.setActiveSchemaFilePath(ValueProvider.StaticValueProvider.of("gs://" +
                        triggeredBucketName + options.getSchemaFolderPath().toString() + getFileNameWithExtension(triggeredDataFileName)));
                options.setActiveBiqQueryTableName(ValueProvider.StaticValueProvider.of(options.getProjectID().toString() +
                        ":" + options.getBqDataSetName().toString() + "." + getOnlyFileName(triggeredDataFileName)));

                LOG.info("[INFO] Loading data file " + options.getActiveDataSetFilePath() +
                        " with schema " + options.getActiveSchemaFilePath() +
                        " to Google BigQuery table " + options.getActiveBiqQueryTableName() + ".");

                processContext.output(options.getActiveDataSetFilePath().toString());
            }
            else {
                //  Set pipeline options parameters to the marker variable value indicating that data load isn't needed.
                options.setActiveDataSetFilePath(ValueProvider.StaticValueProvider.of(MARKER));
                options.setActiveSchemaFilePath(ValueProvider.StaticValueProvider.of(MARKER));
                options.setActiveBiqQueryTableName(ValueProvider.StaticValueProvider.of(MARKER));

                LOG.info("[INFO] Skipping data upload to Google BigQuery...");
            }
        }

        /**  Comparative logic on data upload to Google BigQuery decision here. */
        private boolean checkForCoincidenceTest (String triggeredDataFileName,
                                                 String triggeredBucketName,
                                                 String triggeredDataFileExtension,
                                                 String observedDataFolderPath,
                                                 String observedBucketName,
                                                 String observedDataFileExtension) {

            return ((triggeredDataFileName.contains(observedDataFolderPath)) &&
                    (Objects.equals(triggeredBucketName, observedBucketName)) &&
                    (Objects.equals(triggeredDataFileExtension, observedDataFileExtension)));
        }


        public static String getFileNameWithExtension(String path) {

            return (new File(path).getName());
        }

        public static String getOnlyFileName(String path) {

            return FilenameUtils.getBaseName(path);
        }

        public static String getOnlyFileExtension(String filename) {

            return FilenameUtils.getExtension(filename);
        }

    }

    /** Download data from Google CS or return empty object if the download isn't needed */
    public static class DataLoader extends DoFn<String, String> {

        @ProcessElement
        public void processElement (ProcessContext processContext) {
            LOG.info("[INFO] File name " + processContext.element() + " will be uploaded.");

            //  Get file content from GCS and parse into lines
            Storage storage = StorageOptions.getDefaultInstance().getService();

            Options options = processContext.getPipelineOptions().as(Options.class);

            Blob blob = storage.get(BlobId.of(options.getBucketName().toString(),
                    options.getActiveDataSetFileNameWithPath().toString()));

            byte[] content = blob.getContent(Blob.BlobSourceOption.generationMatch());

            String jsonContent = new String(content);
            String[] jsonLines = jsonContent.split("\n");

            for (String line : jsonLines) {
                processContext.output(line);

                LOG.info("[INFO] JSON's line is : " + line);
            }

        }

    }

    /** Converts UTF8 encoded Json records to TableRow records. */
    private static class JsonToTableRow
            extends PTransform<PCollection<String>, PCollection<TableRow>> {

        @Override
        public PCollection<TableRow> expand(PCollection<String> stringPCollection) {
            return stringPCollection.apply(
                    "JsonToTableRow",
                    MapElements.via(
                            new SimpleFunction<String, TableRow>() {
                                @Override
                                public TableRow apply(String json) {
                                    return convertJsonToTableRow(json);
                                }
                            }));
        }
    }

    /** Factory method for {@link GCSToBigQueryStreamingPipeline.JsonToTableRow}. */
    public static PTransform<PCollection<String>, PCollection<TableRow>> jsonToTableRow() {
        return new GCSToBigQueryStreamingPipeline.JsonToTableRow();
    }

    /**
     * Converts a JSON string to a {@link TableRow} object. If the data fails to convert, a {@link
     * RuntimeException} will be thrown.
     *
     * @param json The JSON string to parse.
     * @return The parsed {@link TableRow} object.
     */
    public static TableRow convertJsonToTableRow(String json) {
        TableRow row;
        // Parse the JSON into a {@link TableRow} object.
        try (InputStream inputStream =
                     new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            row = TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);

        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize json to table row: " + json, e);
        }

        return row;
    }

}
