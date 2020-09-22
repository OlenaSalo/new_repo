package com.equifax.apireporting.pipelines;

import com.equifax.apireporting.pipelines.utils.JdbcConverters;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class CloudSQLToBigQueryPipeline {

    public static void main(String[] args) {

        JdbcConverters.CloudSQLToBigQueryOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(JdbcConverters.CloudSQLToBigQueryOptions.class);

        run(options);
    }

    private static PipelineResult run(JdbcConverters.CloudSQLToBigQueryOptions options) {
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply(
                        "Read from CloudSQL",
                        JdbcIO.<TableRow>read()
                                .withDataSourceConfiguration(
                                        JdbcIO.DataSourceConfiguration.create(
                                                options.getDriverClassName(),
                                                options.getConnectionURL()
                                        ).withUsername(options.getUsername()
                                        ).withPassword(options.getPassword())
                                )
                                .withQuery(options.getQuery())
                                .withCoder(TableRowJsonCoder.of())
                                .withRowMapper(JdbcConverters.getResultSetToTableRow()))
                .apply(
                        "Write to BigQuery",
                        BigQueryIO.writeTableRows()
                                .withoutValidation()
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                                .to(options.getBigQueryOutputTable())
                );
        return pipeline.run();
    }
}
