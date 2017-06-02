#!/bin/bash

basedir=$(dirname $0)

CLASSPATH=""

for f in `find -L $basedir -name "*.jar"`;
do
    CLASSPATH="$CLASSPATH":"$f"
done

if [ -z "$JVM_OPTS" ]
then
    JVM_OPTS="-Xmx1G -Djava.net.preferIPv6Addresses=true"
fi

if [ -n "$JAVA_HOME" ]
then
    JAVA="$JAVA_HOME/bin/java"
elif [ -z "$JAVA" ]
then
    JAVA=java
fi

if [ -n "$LOG_DIR" ]
then
	JVM_OPTS="$JVM_OPTS -Dlogs.dir=$LOG_DIR -XX:+UseG1GC -XX:MaxGCPauseMillis=10"
fi

if [ -n "$LOG4J" ]
then
	JVM_OPTS="$JVM_OPTS -Dlog4j.configuration=$LOG4j"
else
	JVM_OPTS="$JVM_OPTS -Dlog4j.configuration=file:$basedir/etc/log4j.properties"
fi

if [ -n "$JVM_DEBUG_PORT" ]
then
	JVM_OPTS="$JVM_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=$JVM_DEBUG_PORT"
fi

echo "CLASSPATH=$CLASSPATH"
echo "JVM_OPTS=$JVM_OPTS"
export CLASSPATH

exec $JAVA $JVM_OPTS ru.yandex.yt.ytclient.examples.SelectRowsBenchmark $@

