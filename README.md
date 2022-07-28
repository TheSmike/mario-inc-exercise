# Mario-inc Weather Data 

## Introduction
At Mario's devices Inc. we have developed a set of devices which can monitor weather conditions and air
quality. The information about the devices is collected by a microservice that stores the data into a sql
database. A process exports the data weekly from the sql database as a CSV file and stores it in a
landing zone. The CSV contains information about all the installed devices and is replaced at each
execution.

The deployed devices monitor the environmental conditions and send the acquired samples (as soon as
possible) to a message bus. Periodically, a process reads the messages from the bus and writes the
received samples to our landing zone. A new folder is created for each day with the
`received=yyyy-mm-dd` template and the data is stored in `ndjson` format.

The goal of this project is to define pipelines to collect data from these devices and save them into a Data Lake. 

## Overview of the project

The project contains 5 data pipelines, one for each phase of the data processing:
 - `CreateTablesPipeline` - creates SQL data structures if they don't exist.
 - `DeviceInfoPipeline` - loads info data.
 - `RawDeviceDataPipeline` - loads device data from json source "as is". 
 - `DeviceDataPipeline` - reads from Raw Data table and refines data: filtering and cleaning them and renaming some 
columns.
 - `ReportPipeline` - aggregates device data by month to show average numeric values.

All the tables generated and loaded are DeltaTable.

## Project structure

All the pipelines in the project should extend SparkApp class which define the main method to run a Spark job and some
common point like: logging, Session init, config init, input arguments parsing, etc.
When you extend SparkApp you have to define and then specify a derived class of `AbstractContext`, it helps you to handle
input arguments, it contains 2 fixed arguments: 
 - profile: it defines which config files to use and how to create the SparkSession
 - force: it defines if the pipeline must work in force mode (explained below)  

A class that extends SparkApp could and should use:
```
val config: AppConfig
val logger: Logger
val session: SparkSession 
```

The `AppConfig` class map all config values of the project from the file `application-${profile}.conf` from 
`src/main/resources/` folder. The file is selected at runtime based on the input arguments `--profile` ( if the pipeline
class correctly extend SparkApp, profile is a mandatory field thanks to AbstractContext).
**Note**: Edit `ConfigReader.read()` method each time a new config is added to the config files.

All the pipelines are located in `it.scarpenti.marioinc.pipeline`, each one in a dedicated package, and by convention 
all of them have the same structure:
 - `[PipelineName]Context` - See `AbstractContext` above
 - `[PipelineName]Pipeline` - initialize business logic class, and in case input DS/DF/RDD 
 - `[PipelineName]Logic` - a class containing the business logic of the pipeline 

The last one is a convention to make business logic more modular and then testable (see notes in the last section about
this choice).

In the model package there are the beans for the data models, they also contain the constants relative to the name 
of the columns, please use these const when you refer to column names to avoid typo and to facilitate rename operations.

Use `SparkSessionFactory.getSession` to get the SparSession, it should only serve in tests, it's provided by SparkApp in
a standard Pipeline. The method provide a singleton instance of the session and check if an incorrect profile is passed
in subsequent call (it throws error in this case).

DateUtils contains useful static function to support operations and conversions on date, it wraps and synthesize some
standard date function lib, use them and implement here new date functions.

## Pipelines in detail
As said above, all pipelines need the profile parameter as input and all of them accept the optional flag force. Here an 
example of how to set these 2 parameters: `--profile=local -f`, for the sake of simplicity these parameters will not be 
reported further in the sections below. To parse the input parameters the 
[clist lib](https://github.com/backuity/clist) has been used, refer to its page to know all the methods of use to pass
args to the pipelines.   

### CreateTablesPipeline
This pipeline creates database and tables, it creates them if they don't exist. You can use the force mode to replace
everything.

**Parameters:**
no parameters expected here except the standard ones.

### DeviceInfoPipeline
It reads data from landing zone and loads them in the info delta table.
Parameters: no parameters expected here except the standard ones.

### RawDeviceDataPipeline
It reads data from a specific folder (received_date) from the landing zone and loads them in the raw data delta table.
This table is partitioned by event date that is computed from timestamp field. (It should also be sorted by device to 
optimize query on device).

**Parameters:**
```
<received-date> : Received date to process in the form yyyy-MM-dd. i.e.: 2021-01-01. it corresponds to a landing zone folder
```
### DeviceDataPipeline
It reads data from the raw data delta table filtered by received_date and loads them in the device data delta table.
This table is partitioned by event date that is computed from event_timestamp field. (It should also be sorted by device 
to  optimize query on device). Note: This pipeline exploits delta table merge function to avoid duplicates data on 
different partitions.

**Parameters:**
```
<received-date> : Received date to process in the form yyyy-MM-dd. i.e.: 2021-01-01. It's used to filter raw data.
```

### ReportPipeline
It reads data from device and info tables, join them and aggregates by month to obtain average values of numeric 
columns.

**Parameters:**
```
<year-month-from> : The starting month from which to calculate the report, in the form yyy-MM. i.e.: 2020-01
<year-month-to> : The last month to use to calculate the report. It will be in the form yyy-MM. i.e.: 2020-11
```


## Tests

There is a sample of device and info data into the `src/test/resources/test_data/sample_data` folder. Each other folder
in `src/test/resources/test_data/` should be used to a specific test case (like `duplicates_on_different_dates`).
Consider using `SparkSessionFactory.getSession(LOCAL)` to make your own test. 


## Example of usage

To launch pipelines execute the usual command spark-submit specifying class and pipeline parameters as said above. 
The launch command template is: 

```shell
spark-submit --deploy-mode client {SPARK parameters} \
--class {PACKAGE}.{PIPELINE_CLASS} \
s3://path-to-jar/mario-inc-exercise2.12-{VERSION}.jar \
--profile=staging [-f] \
{OTHER SPECIFIC PIPELINE PARAMETERS}
```

An example with `DeviceDataPipeline`:
```shell
spark-submit --deploy-mode client --driver-cores 4 --driver-memory 8G --executor-cores 3 --executor-memory 12G \
--class it.scarpenti.marioinc.pipeline.data.DeviceDataPipeline \
s3://path-to-jar/mario-inc-exercise2.12-0.1.0.jar \
--profile=staging \
--received-date=2021-04-01
``` 

## CI / CD
Below some notes about the CI/CD and repository settings:
 - I'm used to using gitflow, then normally I use master, develop, hotfix and feature branches. A review process must be
   applied on each PR to develop and master.
 - The CI/CD has been set with github workflows, its config is in `.github/workflows/scala.yml` and it's very simple:
   it runs test on every push or pull request on master branch.
 - I used to set CI/CD to package a fat jar with all the dependency, deploy it into an artifact repository (like Nexus)
   and copy it into a DFS like S3 o HDFS. I am also used to upgrading the project version in the master branch and finally
   align develop branch with it.  
 - When the final artifact is created the CI/CD should also copy the right config files (production) into the
   artifact.

## Some personal notes on the exercise

There are some stuff we can do to improve this project, some of these improvements are reported inline in the code. 
Some other notes are reported below: 


 - I'm used to generating a class for each step of the pipeline: read, write and transformation steps. I simply 
   encapsulate each part into a method in the corresponding class for this exercise because it could seem a bit overkill
   for the purpose of this test.
 - I'm used to adding comments to the columns when I create tables (I didn't do it for this exercise).
 - The force mode has been partially (slightly) introduced. In the final version force mode should allow rewriting data 
   generated with that specific run (drop data generated with the same input parameters and regenerate them).
 - It's a good practice to handle range of data as input of the pipeline, it's allow us calculating multiple partition
   at a time and to facilitate possible backfill of data. 
 - In this project I chose to create Logic classes (instead of objects), then with an internal status with inside 
session and config. At this point we could start a discussion on how to isolate business logic respect config and if we 
prefer OOP respect to functional programming. Just to cite an example of pros and cons:
   "avoid a lot of parameters as input of a method (OOP) vs make it clear who uses what (functional)"
 - All the tables in the pipelines has been read and write by name, obviously except landing zone files that don't have
   corresponding tables.
 - I had some issues with tests using Spark in different test classes, I try to make the Spark session a Singleton and
   to set sbt to run tests sequentially, but it didn't work. I should spend more time to investigate further to separate 
   integration tests and unit tests.
 - Files size is an aspect that should be optimized perhaps
 - The selected partition strategy optimise reading of data by event date. To optimise reading also by device the
   [z-ordering feature](https://docs.databricks.com/delta/optimizations/file-mgmt.html#z-ordering-multi-dimensional-clustering)
   of delta lake has been selected (it has not been actually implemented for reasons of time and due to an issue 
   that doesn't allow me to use the command on the local Spark session).
   - In addition, other partition strategies could be evaluated based on the use cases.