# Flink-RDF-EntityRank
RDF-EntityRank Flink Library.

## Description
RDF-EntityRank Flink computes the term-frequency/inverse document frequency for the given DataSet of documents. The DataSet will be treated as a corpus of documents.

## Flink-RDF main application class
The main application class is `net.sansa.entityrank.flink.Job`.
The application requires as application arguments:

1. path to the input folder containing the RDF data as nt (e.g. `/data/input`)
2. path to the output folder to write the resulting to (e.g. `/data/output`)

All Flink workers should have access to the `/data/input` and `/data/output` directories.

## Running the application on a Flink standalone cluster

To run the application on a standalone Flink cluster

1. Setup a Flink cluster
2. Build the application with Maven

  ```
  cd /path/to/application
  mvn clean package
  ```

3. Run the application to the Flink cluster

  ```
cd /path/to/flink/installation
./bin/flink run -c \
		net.sansa.entityrank.flink.Job \
		/path/to/program/jarfile -arg1 -arg2 ... 
  ```
