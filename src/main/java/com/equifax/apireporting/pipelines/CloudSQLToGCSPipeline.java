package com.equifax.apireporting.pipelines;

import com.equifax.apireporting.pipelines.utils.JdbcConverters;
import com.equifax.apireporting.pipelines.utils.KMSEncryptedNestedValueProvider;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * Pipeline template to get data from CloudSQL and save it to GCS bucket, as new line delimited JSON data file(s).
 */
public class CloudSQLToGCSPipeline {

    private static ValueProvider<String> decryptIfNeeded(
            ValueProvider<String> unencryptedValue, ValueProvider<String> kmsKey) {
        return new KMSEncryptedNestedValueProvider(unencryptedValue, kmsKey);
    }

    public static void main(String[] args) {

        JdbcConverters.CloudSQLToGCSPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(JdbcConverters.CloudSQLToGCSPipelineOptions.class);

        run(options);
    }

    private static PipelineResult run(JdbcConverters.CloudSQLToGCSPipelineOptions options) {
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply(
                        "Read data from CloudSQL",
                        JdbcIO.<TableRow>read()
                                .withDataSourceConfiguration(
                                        JdbcIO.DataSourceConfiguration.create(
                                                options.getDriverClassName(),
                                                options.getConnectionURL())
                                                    .withUsername(options.getUsername())
                                                    .withPassword(options.getPassword())
                                )
                                .withQuery(options.getQuery())
                                .withCoder(TableRowJsonCoder.of())
                                .withRowMapper(JdbcConverters.getResultSetToTableRow()))
                .apply("Convert TableRow to JSON in String format",
                        ParDo.of(
                                new DoFn<TableRow, String>() {
                                    @ProcessElement
                                    public void processElement(@Element TableRow tableRow, OutputReceiver<String> receiver) {
                                        Gson gson = new GsonBuilder().create();
                                        String json = gson.toJson(tableRow);
                                        receiver.output(json);
                                    }
                                }
                        ))
                .apply(
                        "Write data to GCS",
                        TextIO.write()
                                .to(options.getOutputFilePath())
                                .withoutSharding()
                );

        return pipeline.run();
    }
}
