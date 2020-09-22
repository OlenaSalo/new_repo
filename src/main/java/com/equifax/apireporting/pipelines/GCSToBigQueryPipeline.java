package com.equifax.apireporting.pipelines;

import com.equifax.apireporting.pipelines.commons.SchemaParser;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A template that copies data from a GCS bucket to an existing BigQuery table.
 */
public class GCSToBigQueryPipeline {

    /** Options supported by {@link GCSToBigQueryPipeline}. */
    public interface Options extends PipelineOptions {
        @Description("The GCS location of the text you'd like to process")
        ValueProvider<String> getInputFilePattern();

        void setInputFilePattern(ValueProvider<String> value);

        @Description("JSON file with BigQuery Schema description")
        ValueProvider<String> getJSONSchemaPath();

        void setJSONSchemaPath(ValueProvider<String> value);

        @Description("Fully qualified BigQuery table name to write to")
        ValueProvider<String> getOutputTable();

        void setOutputTable(ValueProvider<String> value);

        @Validation.Required
        @Description("Temporary directory for BigQuery loading process")
        ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

        void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);
    }

    private static final Logger LOG = LoggerFactory.getLogger(GCSToBigQueryPipeline.class);

    private static final String BIGQUERY_SCHEMA = "BigQuery Schema";
    private static final String NAME = "name";
    private static final String TYPE = "type";
    private static final String MODE = "mode";
    private static final String FIELDS = "fields";
    private static final String TYPE_RECORD = "RECORD";
    private static final String TYPE_STRUCT = "STRUCT";
    private static final int MAX_NESTING = 15;

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Read from GCS", TextIO.read().from(options.getInputFilePattern()))
                .apply(jsonToTableRow())
                .apply(
                        "Insert into BigQuery",
                        BigQueryIO.writeTableRows()
                                .withSchema(
                                        NestedValueProvider.of(
                                                options.getJSONSchemaPath(),
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

                                                        LOG.info("JSON schema parsing is successful");

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
                                .to(options.getOutputTable())
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
                                .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory()));

        pipeline.run();
    }

    /** Factory method for {@link JsonToTableRow}. */
    public static PTransform<PCollection<String>, PCollection<TableRow>> jsonToTableRow() {
        return new JsonToTableRow();
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
}
