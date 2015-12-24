#!/usr/bin/env bash
export HBASE_OPTS="$HBASE_OPTS -Djava.security.auth.login.config={{HBASE_CONF_DIR}}/jaas.conf"
export HBASE_OPTS="-Xms268435456 -Xmx268435456 -XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:-CMSConcurrentMTEnabled -XX:+CMSIncrementalMode -Djava.net.preferIPv4Stack=true $HBASE_OPTS"
# HBASE_CLASSPASTH={{HBASE_CLASSPATH}}
# JAVA_LIBRARY_PATH={{JAVA_LIBRARY_PATH}}
export HBASE_CLASSPATH=`echo $HBASE_CLASSPATH | sed -e "s|$ZOOKEEPER_CONF:||"`
