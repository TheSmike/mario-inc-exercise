# Problem description

The objective is to create a *structured repository* which defines processing pipelines for a data
processing scenario.

## Context

Mario's devices Inc. has developed a set of devices which can monitor weather conditions and air
quality. These devices have been installed by different customers in different locations. Each
location is inside a particular type of area (_industrial, commercial, residential_). Once installed
at a location devices can't be moved. They can only be discarded at the end of their life.

The information about the devices is collected by a microservice that stores the data into a sql
database. A process exports the data weekly from the sql database as a CSV file and stores it in a
landing zone. The CSV contains information about all the installed devices and is replaced at each
execution.

The deployed devices monitor the environmental conditions and send the acquired samples (as soon as
possible) to a message bus. Periodically, a process reads the messages from the bus and writes the
received samples to our landing zone. A new folder is created for each day with the
`received=yyyy-mm-dd` template and the data is stored in `ndjson` format.

The **landing zone is meant as a buffer to transfer data**. A retention policy may be applied to the
data in the landing zone, or the system could be re-architected at any stage to use a different
technology to transfer the data.

### Known issues

Over time, we have experienced a few issues with data received by devices. The main ones are:

* devices may go offline and not send data to the server for a while;
* devices may send a data point that was already sent.

### Volume of data

For this exercise, we are using a subset of data and devices to write a PoC.

In a real-world scenario, there would be between 50.000 and 100.000 devices, and each device would
send a sample per minute. The software developed in this PoC should be designed so that it would
work with the load of the real-world scenario.

## Data samples

### Devices

```
+-----------+--------------------+-----------+-------------+
|code       |type                |area       |customer     |
+-----------+--------------------+-----------+-------------+
|8xUD6pzsQI |capacitive          |commercial |AB-Service   |
|14QL93sBR0j|resistive           |commercial |Atlanta Group|
|36TWSKiT   |capacitive          |residential|Net-Free     |
|6al7RTAobR |thermal-conductivity|residential|Atlanta Group|
|5gimpUrBB  |resistive           |residential|Net-Free     |
|21sjz5h    |resistive           |commercial |Net-Free     |
|4mzWkz     |capacitive          |commercial |Net-Free     |
|2n2Pea     |capacitive          |commercial |AB-Service   |
|1xbYRYcj   |thermal-conductivity|industrial |AB-Service   |
|15se6mZ    |resistive           |residential|Atlanta Group|
+-----------+--------------------+-----------+-------------+
```

This is an example of the data contained in the `csv` file for the devices. The `code` is a unique
identifier for the device.

### Device data

```
+---------+----------+--------+-----------+------------------------+----------+
|CO2_level|device    |humidity|temperature|timestamp               |received  |
+---------+----------+--------+-----------+------------------------+----------+
|1309     |8xUD6pzsQI|63      |24         |2021-04-01T23:47:55.374Z|2021-04-01|
|1430     |8xUD6pzsQI|71      |16         |2021-04-01T22:15:25.053Z|2021-04-01|
|922      |8xUD6pzsQI|55      |25         |2021-04-01T22:09:15.032Z|2021-04-01|
|829      |8xUD6pzsQI|63      |10         |2021-04-01T22:03:05.010Z|2021-04-01|
|1102     |8xUD6pzsQI|51      |21         |2021-04-01T21:56:54.989Z|2021-04-01|
|896      |8xUD6pzsQI|44      |33         |2021-04-01T21:50:44.967Z|2021-04-01|
|1310     |8xUD6pzsQI|32      |23         |2021-04-01T21:44:34.946Z|2021-04-01|
|1527     |8xUD6pzsQI|62      |32         |2021-04-01T21:07:34.817Z|2021-04-01|
|868      |8xUD6pzsQI|79      |14         |2021-04-01T21:01:24.796Z|2021-04-01|
|1311     |8xUD6pzsQI|52      |20         |2021-04-01T20:30:34.689Z|2021-04-01|
|1481     |8xUD6pzsQI|50      |14         |2021-04-01T20:18:14.646Z|2021-04-01|
|1057     |8xUD6pzsQI|70      |28         |2021-04-01T20:12:04.625Z|2021-04-01|
|1572     |8xUD6pzsQI|71      |15         |2021-04-01T19:53:34.561Z|2021-04-01|
|1537     |8xUD6pzsQI|27      |25         |2021-04-01T19:41:14.518Z|2021-04-01|
|1537     |8xUD6pzsQI|27      |25         |2021-04-01T19:41:14.518Z|2021-04-01|
|1327     |8xUD6pzsQI|72      |23         |2021-04-01T19:04:14.389Z|2021-04-01|
|1586     |8xUD6pzsQI|76      |28         |2021-04-01T18:45:44.325Z|2021-04-01|
|1586     |8xUD6pzsQI|76      |28         |2021-04-01T18:45:44.325Z|2021-04-01|
|927      |8xUD6pzsQI|66      |23         |2021-04-01T18:27:14.261Z|2021-04-01|
|1044     |8xUD6pzsQI|69      |23         |2021-04-01T18:21:04.239Z|2021-04-01|
+---------+----------+--------+-----------+------------------------+----------+
```

This is an example of the data samples which are sent by the devices. The `device` identifier
corresponds to the `code` in the devices `csv`. The `timestamp` is the time of sample acquisition,
as produced by the device, while the `received` date is the date when the sample is received by the
message bus.

## Goals of the Assignment

We want to create a data lakehouse that can be used used as single source of truth for data users
that are interested at looking into device info and recorded data.

Write two [Spark](https://spark.apache.org/) pipelines that ingest and cleanse the data:
* one pipeline to process *device information* (the `csv` file)
* the other to process *device data* (the actual data points recorded by devices).

Pipelines can be composed of multiple steps and have to create the datasets needed to match the
requirements that are given below.

Additionally, as a basic example of report, create a third pipeline that:

* takes as input the cleansed data,
* joins the *device data* with the *device info*,
* aggregates the data per month and area,
* and computes average values.

### Data Users

The data created by the pipelines will serve three main users:

* **Data Scientists**: They run transformations on the cleansed data at scale to aggregate it,
  compute features and train models;
* **Data Analysts**: They run transformations on the cleansed data both at scale and on a periodic
  schedule to build aggregated tables for reporting purposes;
* **Device Developers**: They look into the ingested data to analyse the issues and monitor how
  these issues evolve over time.

### Requirements

This is an overview of the minimum requirements that we want to meet. For convenience, we split them
into requirements for device information and requirements for device data.

In addition to the specific requirements, there are also these common ones:

* each dataset that is made available to the users must be stored using a [delta
  table](https://delta.io/);
* the project must provide configuration values that allow the user to execute it in multiple
  environments (i.e. dev, staging, prod). An example of configuration value could be the path of an
  input (or output) file.

#### Device information

The device information must be refreshed (for the purpose of this test it can be replaced) **every
week**, when a new version of the data is available. There is no specific transformation/cleansing
required.

#### Device data

The data must be ingested from the landing zone into the data lakehouse, cleansed to fix known
issues, and made available for analysis to data analysts and data scientists. It must be updated
**daily**.

The ingested data must be available to the developers of the data acquisition system as well, so
that they can query the data and analyse possible issues.

Moreover, it must be possible to reprocess past days if needed, both for reproducibility and in case
data scientists find out that a data cleansing step has to be added or modified.

This is an example of queries that will be run on the cleansed dataset. These queries *must be
optimized*.

```SQL
SELECT *
FROM datalake.device_data_clean
WHERE timestamp > '2021-03-31' and timestamp < '2021-04-05'
```

```SQL
SELECT *
FROM datalake.device_data_clean
WHERE timestamp > '2021-03-31' and timestamp < '2021-04-05' AND
      device = '6al7RTAobR'
```

Please make sure the implemented solution is scalable by design. Data Analysts and Data Scientist
should be able to run transformations on a vast range of the historical data when needed.

##### Data cleansing

For the purposes of the exercise, the data cleansing step may simply discard any sample which does
not meet the requirements.

###### Late arrivals

Samples which arrive *more than one day later* than expected must be discarded.

For example, given these samples in input

```
+---------+----------+--------+-----------+------------------------+----------+
|CO2_level|device    |humidity|temperature|timestamp               |received  |
+---------+----------+--------+-----------+------------------------+----------+
|903      |6al7RTAobR|72      |17         |2021-04-01T21:13:44.839Z|2021-04-04|
|917      |8xUD6pzsQI|63      |27         |2021-04-01T03:45:21.199Z|2021-04-02|
|1425     |5gimpUrBB |53      |15         |2021-04-01T01:35:50.749Z|2021-04-02|
+---------+----------+--------+-----------+------------------------+----------+
```

the pipeline should discard the first sample and keep the others

```
+---------+----------+--------+-----------+------------------------+----------+
|CO2_level|device    |humidity|temperature|timestamp               |received  |
+---------+----------+--------+-----------+------------------------+----------+
|917      |8xUD6pzsQI|63      |27         |2021-04-01T03:45:21.199Z|2021-04-02|
|1425     |5gimpUrBB |53      |15         |2021-04-01T01:35:50.749Z|2021-04-02|
+---------+----------+--------+-----------+------------------------+----------+
```

###### Duplicates

Samples that have the same `device` and same `timestamp` must be discarded.

For example, given these samples in input

```
+---------+-----------+--------+-----------+------------------------+----------+
|CO2_level|device     |humidity|temperature|timestamp               |received  |
+---------+-----------+--------+-----------+------------------------+----------+
|828      |14QL93sBR0j|72      |10         |2021-04-04T00:18:45.481Z|2021-04-04|
|828      |14QL93sBR0j|72      |10         |2021-04-04T00:18:45.481Z|2021-04-04|
|1357     |6al7RTAobR |25      |30         |2021-04-04T17:03:58.972Z|2021-04-04|
|1005     |2n2Pea     |65      |20         |2021-04-03T01:17:20.685Z|2021-04-03|
|1005     |2n2Pea     |65      |20         |2021-04-03T01:17:20.685Z|2021-04-03|
|1581     |2n2Pea     |96      |21         |2021-04-04T22:12:20.042Z|2021-04-04|
+---------+-----------+--------+-----------+------------------------+----------+
```

the pipeline should discard the first and fourth samples, and keep the others:

```
+---------+-----------+--------+-----------+------------------------+----------+
|CO2_level|device     |humidity|temperature|timestamp               |received  |
+---------+-----------+--------+-----------+------------------------+----------+
|828      |14QL93sBR0j|72      |10         |2021-04-04T00:18:45.481Z|2021-04-04|
|1357     |6al7RTAobR |25      |30         |2021-04-04T17:03:58.972Z|2021-04-04|
|1005     |2n2Pea     |65      |20         |2021-04-03T01:17:20.685Z|2021-04-03|
|1581     |2n2Pea     |96      |21         |2021-04-04T22:12:20.042Z|2021-04-04|
+---------+-----------+--------+-----------+------------------------+----------+
```

### Tests

The solution should contain unit tests that run the pipelines on example data to check that the
output is as expected.

### Continuous integration (optional)

A simple CI should be added to the project. 
This can be something minimal just to build the package and run basic tests for demo purposes.
An example could be:

- run tests after every push;
- (optionally) create a release when required.

The release could be a manual job, or a job that checks the presence of a specific tag (or something
else along these lines).

The release should build the project as an executable artifact that can be submitted to Spark (e.g.,
a wheel if you are using Python) and upload it somewhere (for example, as an artifact in a GitHub
release).

## Example Data

The file `mario_device_data.zip` contains some example data which follows the structure that data
has in the landing zone.

## Deliverables

Package the data pipelines in a structured repository with:

- the possibility to execute tests locally
- the possibility to package the application (in a wheel or JAR) that can be potentially sent to a Spark 
  cluster for execution (the application should be configurable as described in the *Requirements* section)
- a continuous integration as described above (optional)
- a README that presents the project (e.g., what the pipelines do, how to build artifacts and run
  tests, etc.)

There is *no requirement to implement everything in detail*, but the README should state which
shortcuts were taken and what improvements are possible.

The repository must be uploaded on a *private* repo on GitHub and shared with us.
