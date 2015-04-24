YARN Starter
====

[YARN](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html), aka NextGen MapReduce, is awesome for building fault-tolerant distributed applications. 
But writing [plain YARN application](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/WritingYarnApplications.html) 
is far than trivial and might even be a [show-stopper to lots of engineers](http://www.edwardcapriolo.com/roller/edwardcapriolo/entry/yarn_either_it_is_really).

The good news is that a framework to simplify interaction with YARN emerged and joined the Apache foundation: [Apache Twill](http://twill.incubator.apache.org/). 
While still in the incubation phase, the project looks really promising and allow to write (easier to test) 
[Runnable](http://docs.oracle.com/javase/8/docs/api/java/lang/Runnable.html) application and run them on YARN.

The goal of this project is to reduce the entry barrier of writing fault-tolerant YARN applications, mostly leveraging on Twill 
([version 0.5.0-incubating](http://twill.incubator.apache.org/apidocs-0.5.0-incubating/index.html) at the time of writing).


# Simple Twill App Example

[SimpleTwillTest](src/test/java/ch/daplab/yarn/twill/SimpleTwillTest.java) is a really trivial application deployed on YARN using Twill. The application basically prints out 
a "hello world"-like message and quits.

The SimpleTwillTest test class extends [AbstractTwillLauncher](src/test/java/ch/daplab/yarn/twill/AbstractTwillLauncher.java) which takes care of 
starting an embedded [Zookeeper](https://github.com/kafka-dev/kafka/blob/master/core/src/test/scala/unit/kafka/zk/EmbeddedZookeeper.scala) 
server as well as a [MiniYARNCluster](https://svn.apache.org/repos/asf/hadoop/common/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-tests/src/test/java/org/apache/hadoop/yarn/server/MiniYARNCluster.java).


# Multiple Runnable Twill App Example

With Twill you can run several different [TwillRunnable](https://github.com/apache/incubator-twill/blob/master/twill-api/src/main/java/org/apache/twill/api/TwillRunnable.java). 
This example runs 2 different applications:
* 1 [Producer](src/test/java/ch/daplab/yarn/twill/worker/Producer.java), which will produce random number in an [embedded redis](https://github.com/kstyrc/embedded-redis) 
    server (via [rpush](http://redis.io/commands/rpush) command). The producer announce the redis instance via the 
    [Twill Service Discovery](http://twill.incubator.apache.org/apidocs/org/apache/twill/discovery/package-summary.html) feature.
* 3 [Consumer](src/test/java/ch/daplab/yarn/twill/worker/Consumer.java)s, which will discover the redis instance and start reading the messages 
    (via [blpop](http://redis.io/commands/blpop) command).


# Fault Tolerant Twitter Ingestion Twill App

Leveraging on the two previous example, this Twill application is reading data from Twitter firehose using [Twitter4j](http://twitter4j.org/) 
and storing the data into HDFS.

Twitter4j has been wrapped as an [RxJava Observable](http://reactivex.io/RxJava/javadoc/rx/Observable.OnSubscribe.html) object, 
and is attached to and HDFS sink, partitioning the data by yyyy/mm/dd/hh/mm. This will be useful to create hive tables 
later on, with proper partitions.

### Configure it

The Twitter keys and secrets are currently hardcoded in [`TwitterObservable.java`](src/main/java/ch/daplab/yarn/twitter/rx/TwitterObservable.java) (yeah, it's in the [TODO list](https://github.com/daplab/yarn-starter/issues/1) :))
Please store them there *before* building.

### Build it

```
mvn clean install
```

### Run it

And Run it in the [DAPLAB](http://daplab.ch) infrastucture like this:

```
./src/main/scripts/start-twitter-ingestion-app.sh daplab-wn-22.fri.lan:2181
```

By default data is stored under `/tmp/twitter/firehose`, monitor the ingestion process:
```
hdfs dfs -ls -R /tmp/twitter/firehose
...
-rw-r--r--   3 yarn hdfs    7469136 2015-04-24 09:59 /tmp/twitter/firehose/2015/04/24/07/58.json
-rw-r--r--   3 yarn hdfs    6958213 2015-04-24 10:00 /tmp/twitter/firehose/2015/04/24/07/59.json
drwxrwxrwx   - yarn hdfs          0 2015-04-24 10:01 /tmp/twitter/firehose/2015/04/24/08
-rw-r--r--   3 yarn hdfs    9444337 2015-04-24 10:01 /tmp/twitter/firehose/2015/04/24/08/00.json
...
```

That's it, now you can kill the application and see how it will be restarted by YARN!

