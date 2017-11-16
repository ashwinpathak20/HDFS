cd $PWD/NameNode

if test -e name_node_config
	then
		:
else
	touch name_node_config
fi


if test -e block_ip_temp
	then
		:
else
	touch block_ip_temp
fi

javac -d bin src/*/*.java

java -cp $PWD/protobuf-java-2.5.0.jar:bin Namenode.namenode_server

