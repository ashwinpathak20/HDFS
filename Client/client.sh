#!/bin/bash

echo "Client Started"
cd $PWD



javac -d bin src/*/*.java
echo -n ">"
read var1
while [ "$var1" != "exit" ]
do
	java -cp $PWD/protobuf-java-2.5.0.jar:bin Client_hdfs.client $var1
    echo -n ">";
    read var1
done
