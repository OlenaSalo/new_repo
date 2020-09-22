package com.equifax.apireporting.pipelines;

import com.equifax.apireporting.pipelines.utils.JdbcConverters;
import com.equifax.apireporting.pipelines.utils.KMSEncryptedNestedValueProvider;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.*;
import com.google.cloud.firestore.Firestore;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Pipeline template to get data from FireStore and save it to GCS bucket, as new line delimited JSON data file(s).
 */
public class FirestoreToGCSPipeline {

    private static ValueProvider<String> decryptIfNeeded(
            ValueProvider<String> unencryptedValue, ValueProvider<String> kmsKey) {
        return new KMSEncryptedNestedValueProvider(unencryptedValue, kmsKey);
    }

    public static void main(String[] args) {
        JdbcConverters.FirestoreToGCSOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(JdbcConverters.FirestoreToGCSOptions.class);

        run(options);
    }

    private static PipelineResult run(JdbcConverters.FirestoreToGCSOptions options)  {
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(
                "Initialize",
                Create.of(1).withCoder(BigEndianIntegerCoder.of())
        ).apply(
                "Read data from Firestore",
                ParDo.of(
                        new DoFn<Integer, String>() {
                            @ProcessElement
                            public void processElement(
                                    @Element Integer el, OutputReceiver<String> receiver, ProcessContext processContext) throws IOException, ExecutionException, InterruptedException {
                                FirestoreOptions firestoreOptions =
                                        FirestoreOptions.getDefaultInstance().toBuilder()
                                                .setProjectId(processContext
                                                        .getPipelineOptions()
                                                        .as(JdbcConverters.FirestoreToGCSOptions.class)
                                                        .getProjectID()
                                                        .toString())
                                                .setCredentials(GoogleCredentials.getApplicationDefault())
                                                .build();

                                Firestore db = firestoreOptions.getService();

                                ApiFuture<QuerySnapshot> query = db.collection(processContext
                                        .getPipelineOptions()
                                        .as(JdbcConverters.FirestoreToGCSOptions.class)
                                        .getCollectionName()
                                        .toString()).get();

                                QuerySnapshot querySnapshot = query.get();
                                List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();

                                for (QueryDocumentSnapshot document : documents) {
                                    Gson gson = new GsonBuilder().create();
                                    String json = gson.toJson(document.getData());

                                    receiver.output(json);
                                }
                            }
                        }
                )
        ).apply(
                "Write data to GCS",
                TextIO.write()
                        .to(options.getOutputFilePath())
                        .withoutSharding()
        );

        return pipeline.run();
    }
}