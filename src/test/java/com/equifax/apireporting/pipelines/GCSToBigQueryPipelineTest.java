package com.equifax.apireporting.pipelines;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;


@RunWith(JUnit4.class)
public class GCSToBigQueryPipelineTest  {

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testApplyJsonToTableRow() throws JsonProcessingException {
        Map<String, Object> params = new HashMap<>();
        params.put("message", "Hello World");
        String payload = new ObjectMapper().writeValueAsString(params);

        TableRow actualJson = new TableRow()
                .set("message", "Hello World");

        PCollection<String> jsonString = pipeline.apply(Create.of(payload));
        PCollection<TableRow> expectedJson = jsonString.apply(GCSToBigQueryPipeline.jsonToTableRow());
        PAssert.that(expectedJson).containsInAnyOrder(actualJson);
        pipeline.run();
    }

    @Test
    public void testJsonToTableRowGood() throws Exception {
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
                        .apply(GCSToBigQueryPipeline.jsonToTableRow());

        PAssert.that(transformedJson).containsInAnyOrder(expectedRow);

        pipeline.run();
    }

}
