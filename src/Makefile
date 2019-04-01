all : server client

clean :
	rm */*.class

server : HdfsServer.class HidoopServer.class

client : HdfsClient.class HidoopClient.class

HdfsClient.class :
	javac hdfs/HdfsClient.java

HdfsServer.class :
	javac hdfs/HdfsServer.java

HidoopServer.class :
	javac ordo/HidoopServer.java

HidoopClient.class :
	javac ordo/HidoopClient.java
