### Associations de port
8088 : Hadoop cluster

13562 : Incompatible shuffle request version

8030, 8031, 8032, 8033, 45637, 8040 : It looks like you are making an HTTP request to a Hadoop IPC port. This is not the correct port for the web interface on this daemon.

9864, 35275 : Hadoop, Datanode on

8042 : Hadoop, Node Manager

9868 : Hadoop status : Hadoop, 2019.

9870 : Hadoop Overview

hdfs dfs -mkdir /output

(base) kimmeng@matebook:~/DataCloud$ yarn jar WordCount.jar mapreduce/WordCount /loremIpsum /output
