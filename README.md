# Flink-RDF-EntityRank
RDF-EntityRank Flink Library.

## Description
RDF-EntityRank Flink computes the term-frequency/inverse document frequency for the given DataSet of documents. The DataSet will be treated as a corpus of documents.

## Flink-RDF main application class
The main application class is `net.sansa_stack.entityrank.flink.Job`.
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
		net.sansa_stack.entityrank.flink.Job \
		/path/to/program/jarfile -arg1 -arg2 ... 
  ```

## Running the application on a Flink standalone cluster via Flink/HDFS Workbench

Flink/HDFS Workbench Docker Compose file contains HDFS Docker (one namenode and two datanodes), Flink Docker (one master and one worker) and HUE Docker as an HDFS File browser to upload files into HDFS easily. Then, this workbench will play a role as for flink-entityrank application to perform computations.
Let's get started and deploy our pipeline with Docker Compose. 
Run the pipeline:

  ```
docker network create hadoop
docker-compose up -d
  ```
First, let’s throw some data into our HDFS now by using Hue FileBrowser runing in our network. To perform these actions navigate to http://your.docker.host:8088/home. Use “hue” username with any password to login into the FileBrowser (“hue” user is set up as a proxy user for HDFS, see hadoop.env for the configuration parameters). Click on “File Browser” in upper right corner of the screen and use GUI to create /user/root/input and /user/root/output folders and upload the data file into /input folder.
Go to http://your.docker.host:50070 and check if the file exists under the path ‘/user/root/input/yourfile’.

After we have all the configuration needed for our example, let’s rebuild flink-entityrank.

```
docker build --rm=true -t sansa/flink-entityrank .
```
And then just run this image:
```
docker run --name flink-entityrank-app --net hadoop --link flink-master:flink-master \
-e ENABLE_INIT_DAEMON=false \
-e FLINK_MASTER_PORT_6123_TCP_ADDR=flink-master \
-e FLINK_MASTER_PORT_6123_TCP_PORT=6123 \
-d sansa/flink-entityrank
```

## Running the application on a Flink standalone cluster via Docker

To run the application, execute the following steps:

1. Setup a Flink cluster as described on http://github.com/big-data-europe/docker-flink.
2. Build the Docker image: 
`docker build --rm=true -t sansa/flink-entityrank .`
3. Run the Docker container: 
`docker run --name flink-entityrank-app -e ENABLE_INIT_DAEMON=false --link flink-master:flink-master  -d sansa/flink-entityrank`


