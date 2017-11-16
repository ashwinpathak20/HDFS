#!/bin/bash

cd $PWD/DataNode

javac -d bin src/*/*.java


if [ $# -ne 1 ]
then
	echo "Error : enter the argument like 1/2/3/4.."
else

	ip=$1
	java -cp $PWD/protobuf-java-2.5.0.jar:bin Datanode.Datanode_server $ip

fi
