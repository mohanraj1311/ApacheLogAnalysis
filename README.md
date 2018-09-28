# ApacheLogAnalysis

## Runtime Environment
Executed this on my laptop which is a 4 core machine.
I have used Spark because we can distribute the work among the 4 processors.
Same program can be run on multinode hadoop cluster which is usually the case in production. 
We could have also used spark streaming if we need the top users data continously. 

## Why Spark ?
Spark is a fast and general engine for large scaling data processing. 
Basically it can take masssive data sets and distribute the processing across clusters.

## How to run ?

We use "spark-submit" to run the program.
General steps: -

```

spark-submit --class ApacheLogAnalysis AnalysisOfApacheLogs.jar ~/Downloads/logs/

```

## NOTE:

```
In the above example:

ApacheLogAnalysis - Its the class object that contains the main function

AnalysisOfApacheLogs.jar - Its the full path of the jar. Pls give the full path
if you are running from some other directory for example:
    /Users/rajamohanmohanraj/IdeaProjects/AnalysisOfApacheLogs/out/artifacts/AnalysisOfApacheLogs_jar/AnalysisOfApacheLogs.jar

~/Downloads/logs/ - Its the arguemnt to the main program where the sample logs are present.

```

## SAMPLE OUTPUT

```

Rajamohans-MacBook-Pro:~ rajamohanmohanraj$ spark-submit --class ApacheLogAnalysis /Users/rajamohanmohanraj/IdeaProjects/AnalysisOfApacheLogs/out/artifacts/AnalysisOfApacheLogs_jar/AnalysisOfApacheLogs.jar /Users/rajamohanmohanraj/IdeaProjects/AnalysisOfApacheLogs/src/apachelogs
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Total no. of unique users : 38
Top users:
id          # pages # sess  longest shortest
489f3e87     14555     1    860    860
71f28176     8835     1    860    860
95c2fa37     4732     1    860    860
eaefd399     4312     1    860    860
43a81873     3926     3    409    409
Rajamohans-MacBook-Pro:~ rajamohanmohanraj$

```


