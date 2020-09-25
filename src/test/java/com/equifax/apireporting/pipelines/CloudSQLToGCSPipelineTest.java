package com.equifax.apireporting.pipelines;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import java.io.Serializable;



public class CloudSQLToGCSPipelineTest implements Serializable {

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testApplyConvertTableRowToJSON()  {
        TableRow row1 = new TableRow().set("name", "a").set("number", "1");
        TableRow row2 = new TableRow().set("name", "b").set("number", "2");
        TableRow row3 = new TableRow().set("name", "c").set("number", "3");
        PCollection<String> expectedJsonString = pipeline.apply(Create.of(row1, row2, row3))
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
                        ));
        ;
        String jsonStringB = "{\"name\":\"b\",\"number\":\"2\"}";
        String jsonStringA = "{\"name\":\"a\",\"number\":\"1\"}";
        String jsonStringC = "{\"name\":\"c\",\"number\":\"3\"}";


        PAssert.that(expectedJsonString).containsInAnyOrder(jsonStringB, jsonStringA, jsonStringC);
        pipeline.run();
    }
}
