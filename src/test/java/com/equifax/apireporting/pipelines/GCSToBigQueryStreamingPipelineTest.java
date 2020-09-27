package com.equifax.apireporting.pipelines;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static com.equifax.apireporting.pipelines.GCSToBigQueryStreamingPipeline.FlowConfigurator.*;
import static org.junit.Assert.assertEquals;

public class GCSToBigQueryStreamingPipelineTest implements Serializable {


    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();
    @Rule
    public transient TemporaryFolder testFolder = new TemporaryFolder();


    @Test
    public void shouldBackFlowDataSetViaConfiguration() throws IOException {
        GCSToBigQueryStreamingPipeline.Options options = PipelineOptionsFactory.as(GCSToBigQueryStreamingPipeline.Options.class);
        ValueProvider<String> dataFolderPath = ValueProvider.StaticValueProvider.of("streaming_test_data");
        ValueProvider<String> bucketName = ValueProvider.StaticValueProvider.of("fakeBucket");
        options.setBucketName(bucketName);
        options.setDataFolderPath(dataFolderPath);

        Pipeline p = Pipeline.create(options);

        Map<String, Object> params = new HashMap<>();
        params.put("name", "streaming_test_data.json");
        params.put("bucket", "fakeBucket");
        String fakeArrivedMsg = new ObjectMapper().writeValueAsString(params);

        String actualActiveDataSetFilePath = "gs://" + params.get("bucket") + options.getDataFolderPath() + params.get("name");

        PCollection<String> expectedActiveDataSetFilePath = p
                .apply(Create.of(fakeArrivedMsg))
                .apply(ParDo.of(new GCSToBigQueryStreamingPipeline.FlowConfigurator()));
        PAssert.that(expectedActiveDataSetFilePath).containsInAnyOrder(actualActiveDataSetFilePath);
        p.run();
    }

    @Test
    public void shouldBackEmptyDataSetViaFlowConfiguration() throws IOException {
        GCSToBigQueryStreamingPipeline.Options options = PipelineOptionsFactory.as(GCSToBigQueryStreamingPipeline.Options.class);
        ValueProvider<String> dataFolderPath = ValueProvider.StaticValueProvider.of("streaming_test_data");
        ValueProvider<String> bucketName = ValueProvider.StaticValueProvider.of("fakeBucket");
        options.setBucketName(bucketName);
        options.setDataFolderPath(dataFolderPath);

        Pipeline p = Pipeline.create(options);

        Map<String, Object> params = new HashMap<>();
        params.put("name", "streaming.json");
        params.put("bucket", "fakeBucket");
        String fakeArrivedMsg = new ObjectMapper().writeValueAsString(params);

        PCollection<String> expectedActiveDataSetFilePath = p
                .apply(Create.of(fakeArrivedMsg))
                .apply(ParDo.of(new GCSToBigQueryStreamingPipeline.FlowConfigurator()));
        PAssert.that(expectedActiveDataSetFilePath).empty();
        p.run();
    }


    @Test
    public void shouldBackFileNameWithExtension() throws IOException {
        File tempFile = testFolder.newFile("tempJson.json");
        String fileName = getFileNameWithExtension(tempFile.getPath());
        assertEquals(fileName, "tempJson.json");
    }

    @Test
    public void shouldBackOnlyFileName() throws IOException {
        File tempFile = testFolder.newFile("tempJson.json");
        String fileName = getOnlyFileName(tempFile.getPath());
        assertEquals(fileName, "tempJson");
    }

    @Test
    public void shouldBackOnlyFileExtension() throws IOException {
        File tempFile = testFolder.newFile("tempJson.json");
        String fileName = getOnlyFileExtension(tempFile.getPath());
        assertEquals(fileName, "json");
    }

    @Test
    public void shouldApplyJsonToTableRow() throws JsonProcessingException {
        Map<String, Object> params = new HashMap<>();
        params.put("message", "Hello World");
        String payload = new ObjectMapper().writeValueAsString(params);

        TableRow actualJson = new TableRow()
                .set("message", "Hello World");

        PCollection<String> jsonString = pipeline.apply(Create.of(payload));
        PCollection<TableRow> expectedJson = jsonString.apply(GCSToBigQueryStreamingPipeline.jsonToTableRow());
        PAssert.that(expectedJson).containsInAnyOrder(actualJson);
        pipeline.run();
    }

    @Test
    public void shouldConvertJsonToTableRowGood() throws Exception {
        TableRow expectedRow = new TableRow();
        expectedRow.set("firstFake", "Larry");
        expectedRow.set("secondFake", "Anderson");
        expectedRow.set("ageFake", 29);

        ByteArrayOutputStream jsonStream = new ByteArrayOutputStream();
        TableRowJsonCoder.of().encode(expectedRow, jsonStream, Coder.Context.OUTER);
        String expectedJson = new String(jsonStream.toByteArray(), StandardCharsets.UTF_8.name());

        PCollection<TableRow> transformedJson =
                pipeline
                        .apply("Create", Create.of(expectedJson))
                        .apply(GCSToBigQueryStreamingPipeline.jsonToTableRow());

        PAssert.that(transformedJson).containsInAnyOrder(expectedRow);

        pipeline.run();
    }

}
