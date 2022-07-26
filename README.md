# Mario-inc Exercise

## Introduction
Mario's devices Inc. has developed a set of devices which can monitor weather conditions and air
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
 - `CreateTablesPipeline` - creates SQL data structures if not exists.
 - `DeviceInfoPipeline` - loads info data.
 - `RawDeviceDataPipeline` - loads data from json source "as is". The pipeline partitions data by event date, it's 
computed from timestamp field.
 - `DeviceDataPipeline` - reads from Raw Data and refines data: filter (clean) and rename some columns.
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

The `AppConfig` class map all config values of the project from a file `application-${profile}.conf` from 
`src/main/resources/` folder. The file is selected at runtime based on the input arguments `--profile` ( if the pipeline
class correctly extend SparkApp, profile is a mandatory field thanks to AbstractContext).
**Note**: Edit `ConfigReader.read()` method each time a new config is added to the config files.

All the pipelines are located in `it.scarpenti.marioinc.pipeline`, each one in a dedicated package, and by convention 
all of them have the same structure:
 - [PipelineName]Context - See `AbstractContext` above
 - [PipelineName]Pipeline - initialize business logic class, and in case input DS/DF/RDD 
 - [PipelineName]Logic - a class containing the business logic of the pipeline 

The last one is a convention to make business logic more modular and then testable. (see notes in the last section about
this choice).

In the model package there are the beans for the data models, they also contain the constants relative to the name 
of the columns, please use these const when you refer to column names to avoid typo and to facilitate rename operations.

Use `SparkSessionFactory.getSession` to get the SparSession, it should only serve in tests, it's provided by SparkApp in
a standard Pipeline. The method provide a singleton instance of the session and check if an incorrect profile is passed
in subsequent call (it throws error in this case).

DateUtils contains useful static function to support operations and conversions on date, it wraps and synthesize some
standard date function lib, use them and implement here new date functions.

## Pipelines in detail
As said above, all pipeline need profile parameter as input and all of them accept the optional flag force. Here an 
example of how to set these 2 parameters: `--profile=local -f`. To parse the input parameters the 
[clist lib](https://github.com/backuity/clist) has been used, refer to its page to know all the methods to pass args
to the pipelines.   

[//]: # (TODO from here )

### CreateTablesPipeline
This pipeline create database and tables, it creates them if they doesn't exist. You can use the force mode to replace
everything.

Input parameters: 

### DeviceInfoPipeline

### RawDeviceDataPipeline

### DeviceDataPipeline

### ReportPipeline





## Test

The configs of the project are located in `src/main/resources`, there are different files, one for each environment. Only the config relative to the local run (application.conf) was correctly set, the other 2 files are a copy of the first one.
Below the pipelines with a brief explanation and the arguments to pass them: 
 - Create Tables pipeline - creates SQL data structures
   - parameters: none
 - Device Info pipeline - loads info data
   - parameters: none
 - Raw Device Data pipeline - loads data from json source "as is" (bronze data). The pipeline partitions data by received date
   - parameters:
     - `received date`: Set it in the form `yyy-MM-dd`, i.e.: 2021-04-02. The pipeline elaborates only data related to the specified date. 
 - Device Data pipeline - reads from bronze source and refines data: filter (clean), rename, partition by event_date (silver data)
   - `received date`: Set it in the form `yyy-MM-dd`, i.e.: 2021-04-02. It is used to extract a single partition from the bronze source (step above). 
 - Report pipeline - aggregates silver data by month and device to show average numeric values
   - `year_month from`: Set it in the form `yyyMM`, i.e.: 202104
   - `year_month to`: Set it in the form `yyyMM`, i.e.: 202105
   - These parameters filter data to be processed to generate the report. The pipeline doesn't overwrite data outside this range.

There are a lot of stuff we can do to improve this project, most of these improvements are reported inline in the code and concern: settings, scaling optimization, partition strategy. 

Below some notes about the CI/CD and repository settings:
- I'm used to using gitflow, then normally I use master, develop, hotfix and feature branches but only master was used for this simple exercise).
- The CI/CD has been set with github workflows, its config is in `.github/workflows/scala.yml` and it's very simple: it runs test on every push or pull request on master branch.
- I used to set CI/CD to package a fat jar with all the dependency, deploy it into an artifact repository (like Nexus) and copy it into a DFS like S3 o HDFS. I am also used to upgrading the project version in the master branch and finally align develop branch with it.
  - When the final artifact is created the CI/CD should also copy the right config files (production) into the artifact

## Example of usage

Command to launch Device Data pipeline: 
```shell
spark-submit --deploy-mode client --driver-cores 4 --driver-memory 8G --executor-cores 3 --executor-memory 12G --conf spark.driver.maxResultSize=8G --class it.scarpenti.marioinc.pipeline.data.DeviceDataPipeline \
s3://path-to-jar/mario-inc-exercise2.12-0.1.0.jar 2021-04-01
```

For the sake of brevity I don't report the other run commands. 


## Some personal notes on the exercise

I'm used to generating a class for each step of the pipeline: read, write and transformation steps. I simply encapsulate
each part into a method in the corresponding class for this exercise because it could seem a bit overkill for the purpose
of this test.

I'm used to adding comments to the columns when I create tables (I didn't do it for this exercise).

[//]: # (TODO Notes on gitflow? )

force mode? 
range of data?

I used to put primary filters, to select certain partitions for example, into the read method....

I used to testing step and never the entire pipeline with unit tests, so...

 
Functional programming vs OOP --- avoid a lot of parameters as input of a method
  - config vs passing all parameters to method
  - 

holden karau usage lib should be optimized, currently it can be used on more than one test

maxDelay as parameter or as property? depends

path vs table

build a helper class to facilitate tests on spark

PR comments

Use range of dates (from,to) in every pipeline: to allow fast backfill

Integration test - I try to do a separate IT