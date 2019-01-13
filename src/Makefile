all : server client daemon interface console

server : HdfsServer.class

client : HdfsClient.class

daemon : Daemon_dataNode.class

interface : InterfaceMapReduce.class

console : Hidoop_lancement.class

HdfsClient.class :
	javac hdfs/HdfsClient.java

HdfsServer.class :
	javac hdfs/HdfsServer.java

Daemon_dataNode.class :
	javac ordo/Daemon_dataNode.java

InterfaceMapReduce.class :
	 javac ordo/InterfaceMapReduce.java

Hidoop_lancement.class :
	javac ordo/Hidoop_lancement.java
