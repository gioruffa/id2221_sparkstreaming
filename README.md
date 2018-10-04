# id2221 SparkStreaming Assignment

Assignment concerning Spark streaming API, including kafka and cassandra.

The [KafkaSpark class](KafkaSpark.scala) consumes key value messages from kafka and calculates continuously the 
average value for each key in a stateful manner.

For more details please see [the attached pdf](lab3.pdf).

## Getting Started
### Prerequisites
You need to hava kafka and cassandra running on the local host.
Please create also a kafka topic named "avg", further setup instructions cab be found in [the attached pdf](lab3.pdf).

### Compile and Run

Compile the generator
```bash
$ cd generator
$ sbt compile
$ sbt run
```

It will start to generate messages of the form
```
(null,"a,21")
(null,"d,4")
...
```

Compile KafkaSpark using sbt
```bash
$ sbt compile
$ sbt run
```

It will compute the averages continuously and store the result in a cassandra table 
called `avg_space.avg`, where `avg_space` is the name of the keyspace.
Both the keyspace and the table are created by the program, if needed.

## Report
In case you want to know more about the design decision taken, please read the [attached pdf report](lab3.pdf)

