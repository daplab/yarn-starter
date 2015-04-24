#!/bin/bash

if [ "$1" == "" ]
then
    echo "Usage: $0 <zookeeper_connection_sting>"
    echo " where <zookeeper_connection_sting> is the hostname:port of one of the zookeeper server"
    exit 1
fi

# Generate the classpath
export HADOOP_CLASSPATH=$(find target/lib/ -type f -name "*.jar" | grep -v "yarn-starter" | paste -sd:)

# Ensure our libs (mainly specific guava version) has precedence over the one provided by hadoop
export HADOOP_USER_CLASSPATH_FIRST=true

# Standard launch. TwitterToHDFSCli implements Tool interface.
yarn jar target/yarn-starter-*.jar ch.daplab.yarn.twitter.TwitterToHDFSCli --zk.connect $1
