YARN Starter
====

[YARN](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html), aka NextGen MapReduce, is awesome for building fault-tolerant distributed applications.  
But writing [plain YARN application](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/WritingYarnApplications.html)  
is far than trivial and might even be a [show-stopper to lots of engineers](http://www.edwardcapriolo.com/roller/edwardcapriolo/entry/yarn_either_it_is_really).

The good news is that a framework to simplify interaction with YARN emerged and joined the Apache foundation: [Apache Twill](http://twill.incubator.apache.org/).  
While still in the incubation phase, the project looks really promising and allow to write (easier to test)  
[Runnable](http://docs.oracle.com/javase/8/docs/api/java/lang/Runnable.html) application and run them on YARN.

The goal of this project is to reduce the entry barrier of writing fault-tolerant YARN applications, mostly leveraging on Twill.

# Simple Twill App

[ch.daplab.yarn.twill.SimpleTwillTest]() is a really trivial application deployed on YARN using Twill. The application basically print out  
a "hello world"-like message and quit.

The SimpleTwillTest test class extends [ch.daplab.yarn.twill.AbstractTwillLauncher]() which takes care of starting an embedded [Zookeeper]()  
server as well as a [MiniYARNCluster]().

# Multiple Runnable Twill App

# Fault Tolerant Twitter Ingestion 
