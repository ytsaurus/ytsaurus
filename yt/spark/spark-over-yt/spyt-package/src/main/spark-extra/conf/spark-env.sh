#!/usr/bin/env bash

export ARROW_PRE_0_15_IPC_FORMAT=1

if [ -z "$SPYT_CLASSPATH" ] && [ -n "$SPARK_CONF_DIR" ]; then
  SPYT_CLASSPATH=$(cd "$SPARK_CONF_DIR"/..; pwd)/jars/*
  export SPYT_CLASSPATH
fi

if [ -n "$SPYT_CLASSPATH" ]; then
  SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -javaagent:$(ls ${SPYT_CLASSPATH}spark-yt-spark-patch*)"
  export SPARK_SUBMIT_OPTS
fi

if [ -z "$SPARK_LAUNCHER_OPTS" ] && [ -n "$SPARK_SUBMIT_OPTS" ]; then
  export SPARK_LAUNCHER_OPTS=$SPARK_SUBMIT_OPTS
fi
