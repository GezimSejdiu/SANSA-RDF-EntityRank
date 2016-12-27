# SANSA-RDF-EntityRank
> A Distributed In-Memory Ranking Procedure for Entities of a Class.

## Description
SANSA RDF EntityRank is a sub library to read RDF files into [Spark](https://spark.apache.org) or [Flink](https://flink.apache.org) and computes the term-frequency/inverse document frequency for the given RDF datasets. It allows files to reside in HDFS as well as in a local file system and distributes them across Spark RDDs/DataFrames or Flink DataSets.

### [sansa-rdf-entityrank-spark](https://github.com/SANSA-Stack/SANSA-RDF-EntityRank/tree/develop/sansa-rdf-entityrank-spark)
Contains the SANSA RDF EntityRank for [Apache Spark](http://spark.apache.org/).

### [sansa-rdf-entityrank-flink](https://github.com/SANSA-Stack/SANSA-RDF-EntityRank/tree/develop/sansa-rdf-entityrank-flink)
Contains the SANSA RDF EntityRank for [Apache Flink](http://flink.apache.org/).

