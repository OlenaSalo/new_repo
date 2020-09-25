package com.equifax.apireporting.pipelines;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.auth.NoopCredentialFactory;
import org.apache.beam.sdk.extensions.gcp.storage.NoopPathValidator;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.*;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class GCSToBigQueryStreamingPipelineTest implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(GCSToBigQueryStreamingPipelineTest.class);

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    private static Storage storage;
    private static final String BUCKET = RemoteStorageHelper.generateBucketName();
    private static final String BLOB = "blob";


    @Test
    public void testFlowConfigurationPositive() throws IOException {
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
    public void testFlowConfigurationNegative() throws IOException {
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
    public void testDataLoader() throws JsonProcessingException {
//        GCSToBigQueryStreamingPipeline.Options options = PipelineOptionsFactory.as(GCSToBigQueryStreamingPipeline.Options.class);
//        ValueProvider<String> dataFolderPath = ValueProvider.StaticValueProvider.of("streaming_test_data");
//        ValueProvider<String> bucketName = ValueProvider.StaticValueProvider.of("fakeBucket");
//        options.setBucketName(bucketName);
//        options.setDataFolderPath(dataFolderPath);


        //Pipeline p = Pipeline.create(options);
        String[] args =
                new String[] {
                        "--runner=DirectRunner",
                        "--tempLocation=/tmp/testing",
                        "--project=test-project",
                        "--credentialFactoryClass=" + NoopCredentialFactory.class.getName(),
                        "--pathValidatorClass=" + NoopPathValidator.class.getName(),
                };
        // Should not crash, because gcpTempLocation should get set from tempLocation
     //   pipeline = TestPipeline.fromOptions(PipelineOptionsFactory.fromArgs(args).create());
        Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).create());
        Map<String, Object> params = new HashMap<>();
        params.put("name", "streaming.json");
        params.put("bucket", "fakeBucket");
        String fakeArrivedMsg = new ObjectMapper().writeValueAsString(params);

        PCollection<String> expectedActiveDataSetFilePath = p
                .apply(Create.of(fakeArrivedMsg))
                .apply(ParDo.of(new GCSToBigQueryStreamingPipeline.DataLoader()));
        PAssert.that(expectedActiveDataSetFilePath).empty();
        p.run();
    }

    @Test
    public void testPathValidatorOverride() {
        String[] args =
                new String[] {
                        "--runner=DirectRunner",
                        "--tempLocation=/tmp/testing",
                        "--project=test-project",
                        "--credentialFactoryClass=" + NoopCredentialFactory.class.getName(),
                        "--pathValidatorClass=" + NoopPathValidator.class.getName(),
                };
        // Should not crash, because gcpTempLocation should get set from tempLocation
        TestPipeline.fromOptions(PipelineOptionsFactory.fromArgs(args).create());
    }
}
