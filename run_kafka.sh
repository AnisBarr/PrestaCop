cd ./kafka_2.12-2.1.0
hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh

hdfs dfs -mkdir /tmp
hdfs dfs -mkdir /storage
hdfs dfs -mkdir /storage/message
hdfs dfs -mkdir /storage/history
hdfs dfs -mkdir /storage/violation
hdfs dfs -mkdir /storage/violation/image_violation


bin/zookeeper-server-start.sh config/zookeeper.properties &
bin/kafka-server-start.sh config/server.properties &
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1  --topic drone_topic &
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1  --topic history &
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1  --topic image_violation &
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1  --topic alert_topic 


# hdfs dfs -ls /storage/violation/image_violation
# hdfs dfs -ls /storage/message


