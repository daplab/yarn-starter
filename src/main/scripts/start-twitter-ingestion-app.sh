#!/bin/bash

# Generate the classpath
export HADOOP_CLASSPATH=$(find target/lib/ -type f -name "*.jar" | grep -v "yarn-starter" | paste -sd:)

# Ensure our libs (mainly specific guava version) has precedence over the one provided by hadoop
export HADOOP_USER_CLASSPATH_FIRST=true

# Standard launch. TwitterToHDFSCli implements Tool interface.
yarn jar target/yarn-starter-*.jar ch.daplab.yarn.twitter.TwitterToHDFSCli $@
