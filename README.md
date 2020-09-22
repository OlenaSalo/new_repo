-- [DONE] Fat JAR build (build with maven):
mvn -P dataflow-runner clean package

-- [DONE] CloudSQLToGCSPipeline start (run with maven) example:
mvn -P dataflow-runner compile exec:java -Dexec.mainClass=com.equifax.apireporting.pipelines.CloudSQLToGCSPipeline -Dexec.args="--project=crucial-oarlock-283420 --runner=DataflowRunner --region=us-east1 --gcpTempLocation=gs://java-templates/stage --connectionURL=jdbc:mysql://10.120.192.3:3306/test --driverClassName=com.mysql.jdbc.Driver --username=test --password=test1234 --query='SELECT * FROM user' --outputFilePath=gs://java-templates/cloudsql/cloudsql-data.json --usePublicIps=true --subnetwork=https://www.googleapis.com/compute/v1/projects/crucial-oarlock-283420/regions/us-east1/subnetworks/apigee-mock-vpc-network"

-- [DONE] CloudSQLToGCSPipeline start (run with maven) example:
java -cp api-reporting-pipelines-complex-bundled-0.0.0.1.jar com.equifax.apireporting.pipelines.CloudSQLToGCSPipeline --project=crucial-oarlock-283420 --runner=DataflowRunner --region=us-east1 --gcpTempLocation=gs://java-templates/stage --connectionURL=jdbc:mysql://10.120.192.3:3306/test --driverClassName=com.mysql.jdbc.Driver --username=test --password=test1234 --query='SELECT * FROM user' --outputFilePath=gs://java-templates/cloudsql/cloudsql-data.json --usePublicIps=true --subnetwork=https://www.googleapis.com/compute/v1/projects/crucial-oarlock-283420/regions/us-east1/subnetworks/apigee-mock-vpc-network 

-- [DONE] FirestoreToGCSPipeline pipeline (java run) example:
mvn -P dataflow-runner compile exec:java -Dexec.mainClass=com.equifax.apireporting.pipelines.FirestoreToGCSPipeline -Dexec.args="--project=crucial-oarlock-283420 --runner=DataflowRunner --region=us-east1 --gcpTempLocation=gs://java-templates/stage --projectID=crucial-oarlock-283420 --collectionName=cities --outputFilePath=gs://java-templates/firestore/firestore-data.json"

-- [DONE] FirestoreToGCSPipeline start (java run) example:
java -cp api-reporting-pipelines-complex-bundled-0.0.0.1.jar com.equifax.apireporting.pipelines.FirestoreToGCSPipeline --project=crucial-oarlock-283420 --runner=DataflowRunner --region=us-east1 --gcpTempLocation=gs://java-templates/stage --projectID=crucial-oarlock-283420 --collectionName=cities --outputFilePath=gs://java-templates/firestore/firestore-data.json

-- [DONE] GCSToBigQueryPipeline start (run with maven) example:
mvn -P dataflow-runner compile exec:java -Dexec.mainClass=com.equifax.apireporting.pipelines.GCSToBigQueryPipeline -Dexec.args="--project=crucial-oarlock-283420 --runner=DataflowRunner --region=us-east1 --gcpTempLocation=gs://java-templates/stage  --bigQueryLoadingTemporaryDirectory=gs://java-templates/temp --JSONSchemaPath=gs://java-templates/firestore/schema/city.json --inputFilePattern=gs://java-templates/firestore/output-00000-of-00001.json --outputTable=crucial-oarlock-283420:firestore_test.cities --usePublicIps=true --subnetwork=https://www.googleapis.com/compute/v1/projects/crucial-oarlock-283420/regions/us-east1/subnetworks/apigee-mock-vpc-network"

-- [DONE] GCSToBigQueryPipeline start (java run) example:
java -cp api-reporting-pipelines-complex-bundled-0.0.0.1.jar com.equifax.apireporting.pipelines.GCSToBigQueryPipeline --project=crucial-oarlock-283420 --runner=DataflowRunner --region=us-east1 --JSONSchemaPath=gs://java-templates/firestore/schema/city.json --inputFilePattern=gs://java-templates/firestore/output-00000-of-00001.json --bigQueryLoadingTemporaryDirectory=gs://java-templates/temp --gcpTempLocation=gs://java-templates/stage --outputTable=crucial-oarlock-283420:firestore_test.cities --usePublicIps=true --subnetwork=https://www.googleapis.com/compute/v1/projects/crucial-oarlock-283420/regions/us-east1/subnetworks/apigee-mock-vpc-network

-- [DONE] CloudSQLToBigQueryPipeline start (run with maven) example:
mvn -P dataflow-runner compile exec:java -Dexec.mainClass=com.equifax.apireporting.pipelines.CloudSQLToBigQueryPipeline -Dexec.args="--project=crucial-oarlock-283420  --runner=DataflowRunner --region=us-east1 --gcpTempLocation=gs://java-templates/stage --connectionURL=jdbc:mysql://10.120.192.3:3306/test --driverClassName=com.mysql.jdbc.Driver --username=test --password=test1234 --query='SELECT * FROM user' --bigQueryLoadingTemporaryDirectory=gs://java-templates/temp --bigQueryOutputTable=crucial-oarlock-283420:test.user3 --usePublicIps=true --subnetwork=https://www.googleapis.com/compute/v1/projects/crucial-oarlock-283420/regions/us-east1/subnetworks/apigee-mock-vpc-network"

-- [DONE] CloudSQLToBigQueryPipeline start (java run) example:
java -cp api-reporting-pipelines-complex-bundled-0.0.0.1.jar com.equifax.apireporting.pipelines.CloudSQLToBigQueryPipeline --project=crucial-oarlock-283420  --runner=DataflowRunner --region=us-east1 --gcpTempLocation=gs://java-templates/stage --connectionURL=jdbc:mysql://10.120.192.3:3306/test --driverClassName=com.mysql.jdbc.Driver --username=test --password=test1234 --query='SELECT * FROM user' --bigQueryLoadingTemporaryDirectory=gs://java-templates/temp --bigQueryOutputTable=crucial-oarlock-283420:test.user3 --usePublicIps=true --subnetwork=https://www.googleapis.com/compute/v1/projects/crucial-oarlock-283420/regions/us-east1/subnetworks/apigee-mock-vpc-network

-- [DONE] GCSToBigQueryStreamingPipeline start (run with maven) example:
mvn -P dataflow-runner compile exec:java -Dexec.mainClass=com.equifax.apireporting.pipelines.GCSToBigQueryStreamingPipeline -Dexec.args="--project=crucial-oarlock-283420 --runner=DataflowRunner --region=us-east1 --gcpTempLocation=gs://java-templates/stage --bigQueryLoadingTemporaryDirectory=gs://java-templates/temp --usePublicIps=true --subnetwork=https://www.googleapis.com/compute/v1/projects/crucial-oarlock-283420/regions/us-east1/subnetworks/apigee-mock-vpc-network --inputTopic=projects/crucial-oarlock-283420/topics/test --inputFilePattern=gs://pubsub1234/data.json"

-- [DONE] JSON's schema example (note the array, named "BigQuery Schema"):
{
  "BigQuery Schema": [
    {
      "mode": "NULLABLE",
      "name": "country",
      "type": "STRING"
    },
    {
      "mode": "NULLABLE",
      "name": "name",
      "type": "STRING"
    },
    {
      "mode": "NULLABLE",
      "name": "state",
      "type": "STRING"
    }
  ]
}
