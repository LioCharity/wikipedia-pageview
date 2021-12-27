# Run unit tests
```
> cd wikipedia-pageview
> sbt clean test
```
# Build a Fat JAR
```
> sbt assembly
```
the Fat JAR will be in target/scala-2.12 folder 

# Inputs
The input to pass to the program must be [PATH ROOT] and the rest of the structure will be built at runtime
[PATH ROOT]/2021/2021-12/pageviews-20211201-000000.gz

# Outputs
The output will be a file per day and hour of processing and will be stored in a folder with the structure:
[OUTPUT ROOT]/2021/2021-01/01/0000/

[OUTPUT ROOT] must be passed to the program and the rest of the structure will be built at runtime.

# How to run the application

## The input parameters
```
[Required]  -i **or** --inputPath <value>  input path

[Optional]  -s  **or** --startDate <value>  start date in format yyy-mm-dd
  
[Optional]  -h, **or** --startHour <value>  start hour in format hh:mm
  
[Optional]  -e, **or** --endDate <value>    end date in format yyy-mm-dd
  
[Optional] -j, **or** --endHour <value>    end hour in format hh:mm
  
[Required]  -o, **or** --outputPath <value> output path
  
[Optional]  -b, **or** --pathToBlacklist <value>  path to black list
  
[Required]  -r, **or** --isRange <value>    Specified if it is a range processing or not. The default is false

[Optional]  -m, **or** --master <value>   Spark master
```

## To process a specified day and hour
the startDate and the startHour must be provided

## To process a range of hours in the same date
the startDate, the startHour and the endHour must be provided

## To process a range of hours in a range of date
startDate, startHour, endDate, endHour must be provided

## To process a range of date
startDate and endDate must be provided. All the hours of the days included in the range will be processed.

## Default values case
- If no date nor time are passed the defaults (date/hour - 24 hours)  date and hour are used.
- if range is set to true
    - if no startDate is provided, the previous day, all the hours are processed.
    - If only the start date is provided, all the hours of the specified day are processed.

**Note:** Other cases than the one listed here are not covered

## Spark submit

Example of Spark submit - No range processing
```
spark-submit  
--class com.lsouoppekam.MainApp 
--master local[10]  
--num-executors 15 
--driver-memory 5G 
--executor-memory 4G  
/Users/lsouoppekam/projects/datadog/wikipedia-pageview/target/scala-2.12/wikipedia-pageview-assembly-0.1.jar 
-r false 
-s 2021-02-01 
-h 00:00 
-i /Users/lsouoppekam/projects/datadog/wikipedia-pageview/src/test/resources 
-o /Users/lsouoppekam/projects/datadog 
-b /Users/lsouoppekam/projects/datadog/wikipedia-pageview/src/test/resources/blacklist/blacklist_domains_and_pages
```
Example of Spark submit - Range processing

```
spark-submit  
--class com.lsouoppekam.MainApp 
--master local[10]  
--num-executors 15 
--driver-memory 5G 
--executor-memory 4G  
/Users/lsouoppekam/projects/datadog/wikipedia-pageview/target/scala-2.12/wikipedia-pageview-assembly-0.1.jar 
-r true 
-s 2021-02-01 
-h 00:00 
-j 03:00 
-i /Users/lsouoppekam/projects/datadog/wikipedia-pageview/src/test/resources 
-o /Users/lsouoppekam/projects/datadog 
-b /Users/lsouoppekam/projects/datadog/wikipedia-pageview/src/test/resources/blacklist/blacklist_domains_and_pages
```

# Processing pipeline description
- Step 1 - parse and validate the input
- Step 2 - generate the input files to be processed
- step 3 - Read the input and for each input,
    - Compute the top 25 page
    - Write the result in a single file in CSV format

# For production
To operate this application in production:
- Have a cloud platform
- Run the application with a correct Spark master like yarn instead of local
- Set up spark core memory, the parallelism: Set spark Configuration
- Have a monitoring tool for alerting in case of failure, unexpected data pattern


# To run automatically for each hour of the day
Nothing is to be change in the application itself, but a scheduler will need to be used. like:
- Oozie
- Dataflow
- control-m


# Improvements
- For now when the input does not exist, the application writes a Warning log (and pass to the next file for range processing).
An improvement can be a monitoring of this, sending metrics to a tools like Grafana or splunk and raise an alert
if there are many missing files.
