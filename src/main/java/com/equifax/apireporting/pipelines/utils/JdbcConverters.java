package com.equifax.apireporting.pipelines.utils;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class JdbcConverters {

  /** Interface used by the JdbcToBigQuery pipeline to accept user input. */
  public interface JdbcToBigQueryOptions extends PipelineOptions {
    @Description(
        "Comma separate list of driver class/dependency jar file GCS paths "
            + "for example "
            + "gs://<some-bucket>/driver_jar1.jar,gs://<some_bucket>/driver_jar2.jar")
    ValueProvider<String> getDriverJars();

    void setDriverJars(ValueProvider<String> driverJar);

    @Description("The JDBC driver class name. " + "for example: com.mysql.jdbc.Driver")
    ValueProvider<String> getDriverClassName();

    void setDriverClassName(ValueProvider<String> driverClassName);

    @Description(
        "The JDBC connection URL string. " + "for example: jdbc:mysql://some-host:3306/sampledb")
    ValueProvider<String> getConnectionURL();

    void setConnectionURL(ValueProvider<String> connectionURL);

    @Description(
        "JDBC connection property string. " + "for example: unicode=true&characterEncoding=UTF-8")
    ValueProvider<String> getConnectionProperties();

    void setConnectionProperties(ValueProvider<String> connectionProperties);

    @Description("JDBC connection user name. ")
    ValueProvider<String> getUsername();

    void setUsername(ValueProvider<String> username);

    @Description("JDBC connection password. ")
    ValueProvider<String> getPassword();

    void setPassword(ValueProvider<String> password);

    @Description("Source data query string. " + "for example: select * from sampledb.sample_table")
    ValueProvider<String> getQuery();

    void setQuery(ValueProvider<String> query);

    @Description(
        "BigQuery Table spec to write the output to"
            + "for example: some-project-id:somedataset.sometable")
    ValueProvider<String> getOutputTable();

    void setOutputTable(ValueProvider<String> value);

    @Description("Temporary directory for BigQuery loading process")
    ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

    void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);

    @Description(
        "KMS Encryption Key should be in the format projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}")
    ValueProvider<String> getKMSEncryptionKey();

    void setKMSEncryptionKey(ValueProvider<String> keyName);
  }

  /** Interface used by the JdbcToBigQuery pipeline to accept user input. */
  public interface CloudSQLToBigQueryOptions extends PipelineOptions {
        @Description("The JDBC driver class name. " + "for example: com.mysql.jdbc.Driver")
    ValueProvider<String> getDriverClassName();

    void setDriverClassName(ValueProvider<String> driverClassName);

    @Description(
        "The JDBC connection URL string. " + "for example: jdbc:mysql://some-host:3306/sampledb")
    ValueProvider<String> getConnectionURL();

    void setConnectionURL(ValueProvider<String> connectionURL);

    @Description(
        "JDBC connection property string. " + "for example: unicode=true&characterEncoding=UTF-8")
    ValueProvider<String> getConnectionProperties();

    void setConnectionProperties(ValueProvider<String> connectionProperties);

    @Description("JDBC connection user name. ")
    ValueProvider<String> getUsername();

    void setUsername(ValueProvider<String> username);

    @Description("JDBC connection password. ")
    ValueProvider<String> getPassword();

    void setPassword(ValueProvider<String> password);

    @Description("Source data query string. " + "for example: select * from sampledb.sample_table")
    ValueProvider<String> getQuery();

    void setQuery(ValueProvider<String> query);

    @Description("Big Query table")
    ValueProvider<String> getBigQueryOutputTable();

    void setBigQueryOutputTable(ValueProvider<String> bigQueryOutputTable);

    @Description("Temporary directory for BigQuery loading process")
    ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

    void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);

    @Description(
        "KMS Encryption Key should be in the format projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}")
    ValueProvider<String> getKMSEncryptionKey();

    void setKMSEncryptionKey(ValueProvider<String> keyName);
  }

  /** Interface used by the CloudSQLToGCSPipeline pipeline to accept user input. */
  public interface CloudSQLToGCSPipelineOptions extends PipelineOptions {
    @Description("GCS file path to write the exported from CloudSQL data." + "for example: gs://output/cloudsql-data.json")
    ValueProvider<String> getOutputFilePath();

    void setOutputFilePath(ValueProvider<String> outputFilePath);

    @Description("The JDBC driver class name. " + "for example: com.mysql.jdbc.Driver")
    ValueProvider<String> getDriverClassName();

    void setDriverClassName(ValueProvider<String> driverClassName);

    @Description(
            "The JDBC connection URL string. " + "for example: jdbc:mysql://some-host:3306/sampledb")
    ValueProvider<String> getConnectionURL();

    void setConnectionURL(ValueProvider<String> connectionURL);

    @Description(
            "JDBC connection property string. " + "for example: unicode=true&characterEncoding=UTF-8")
    ValueProvider<String> getConnectionProperties();

    void setConnectionProperties(ValueProvider<String> connectionProperties);

    @Description("JDBC connection user name. ")
    ValueProvider<String> getUsername();

    void setUsername(ValueProvider<String> username);

    @Description("JDBC connection password. ")
    ValueProvider<String> getPassword();

    void setPassword(ValueProvider<String> password);

    @Description("Source data query string. " + "for example: select * from sampledb.sample_table")
    ValueProvider<String> getQuery();

    void setQuery(ValueProvider<String> query);

    @Description("Big Query table")
    ValueProvider<String> getBigQueryOutputTable();

    void setBigQueryOutputTable(ValueProvider<String> bigQueryOutputTable);

    @Description("Temporary directory for BigQuery loading process")
    ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

    void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);

    @Description(
            "KMS Encryption Key should be in the format projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}")
    ValueProvider<String> getKMSEncryptionKey();

    void setKMSEncryptionKey(ValueProvider<String> keyName);
  }

  /** Interface used by the FirestoreToGCS pipeline to accept user input. */
  public interface FirestoreToGCSOptions extends PipelineOptions {
    @Description("ProjectID to use.")
    ValueProvider<String> getProjectID();

    void setProjectID(ValueProvider<String> projectID);

    @Description("Collection name to use.")
    ValueProvider<String> getCollectionName();

    void setCollectionName(ValueProvider<String> collectionName);

    @Description("GCS file path to write the exported from FireStore data." + "for example: gs://output/firestore-data.json")
    ValueProvider<String> getOutputFilePath();

    void setOutputFilePath(ValueProvider<String> outputFilePath);

    @Description("The JDBC driver class name. " + "for example: com.mysql.jdbc.Driver")
    ValueProvider<String> getDriverClassName();

    void setDriverClassName(ValueProvider<String> driverClassName);

    @Description(
            "The JDBC connection URL string. " + "for example: jdbc:mysql://some-host:3306/sampledb")
    ValueProvider<String> getConnectionURL();

    void setConnectionURL(ValueProvider<String> connectionURL);

    @Description(
            "JDBC connection property string. " + "for example: unicode=true&characterEncoding=UTF-8")
    ValueProvider<String> getConnectionProperties();

    void setConnectionProperties(ValueProvider<String> connectionProperties);

    @Description("JDBC connection user name. ")
    ValueProvider<String> getUsername();

    void setUsername(ValueProvider<String> username);

    @Description("JDBC connection password. ")
    ValueProvider<String> getPassword();

    void setPassword(ValueProvider<String> password);

    @Description("Source data query string. " + "for example: select * from sampledb.sample_table")
    ValueProvider<String> getQuery();

    void setQuery(ValueProvider<String> query);

    @Description("Big Query table")
    ValueProvider<String> getBigQueryOutputTable();

    void setBigQueryOutputTable(ValueProvider<String> bigQueryOutputTable);

    @Description("Temporary directory for BigQuery loading process")
    ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

    void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);

    @Description(
            "KMS Encryption Key should be in the format projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}")
    ValueProvider<String> getKMSEncryptionKey();

    void setKMSEncryptionKey(ValueProvider<String> keyName);
  }

  /** Factory method for {@link ResultSetToTableRow}. */
  public static JdbcIO.RowMapper<TableRow> getResultSetToTableRow() {
    return new ResultSetToTableRow();
  }

  /**
   * {@link JdbcIO.RowMapper} implementation to convert Jdbc ResultSet rows to UTF-8 encoded JSONs.
   */
  private static class ResultSetToTableRow implements JdbcIO.RowMapper<TableRow> {

    @Override
    public TableRow mapRow(ResultSet resultSet) throws Exception {

      ResultSetMetaData metaData = resultSet.getMetaData();

      TableRow outputTableRow = new TableRow();

      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        outputTableRow.set(metaData.getColumnName(i), resultSet.getObject(i));
      }

      return outputTableRow;
    }
  }
}
