#!/bin/bash

ORIGDIR=`pwd`
BASEDIR=$(dirname "$0")
cd $BASEDIR

# Check system reqs
if [[ `type java` ]]; then
    _java=java
elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    _java="$JAVA_HOME/bin/java"
else
    echo "Java not found ... [fail]"
    exit -1
fi

if [[ "$_java" ]]; then
    version=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
    if [[ "$version" > "11" ]]; then
        # TBD enable this for verbose exec
        # echo "Java version is above 11 ... [pass]"
        echo -n ""
    else         
        echo Java is too old, requires Java 11, found "$version" [fail]
        exit -1
    fi
fi

mvn -q exec:exec -Dexec.workingdir=$BASEDIR -Dexec.executable=_java -DADD_ARGS="$*" -Dlog4j.configuration=file:$BASEDIR/src/main/resources/log4j.properties -Dmaven.test.skip=true 2>/dev/null
cd $ORIGDIR
