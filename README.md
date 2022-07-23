# Mario-inc Exercise
An exercise in developing a Big Data project in Spark.

This project should load Mario's devices Inc. data into a data lake (see [here](data_pipelines_exercise_camlin.md)
for the details about requirements).

The project contains 5 pipelines, one for each phase of the data processing.
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



