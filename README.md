Make sure the storm user has permissions to write to your target HDFS directory
```
cd storm-python-example
mvn package

storm jar target/storm-python-example-1.0-SNAPSHOT.jar com.github.randerzander.ExampleTopology kafkaTopicName
```
