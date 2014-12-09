A project to allow dynamic Storm topology definitions. See the example topology.properties file.

With the provided target/storm-python-example.jar, you can add new Python bolts without needing to rebuild the jar.

For Storm to be able to execute Python scripts as bolts, the scripts need to be available inside the jar under the /resources directory.
```
mkdir resources
mv myBoltScript.py resources/
jar uf storm-python-example-1.0-SNAPSHOT.jar resources/myBoltScript.py
```

The only working spout type is the kafka-spout.

The currently working bolt types are:
PyBolts (Python bolts), HBaseBolts, PhoenixBolts, and HDFSBolts


To build manually rebuild the Storm uber jar:
```
mvn package

storm jar target/storm-python-example-1.0-SNAPSHOT.jar com.github.randerzander.ExampleTopology topology.properties
```
