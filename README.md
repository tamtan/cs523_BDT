# cs523_BDT
Final project of Big Data Technology: Spark Streaming with Kafka and Hbase

1.	install kafka and start kafka service

https://www.digitalocean.com/community/tutorials/how-to-install-apache-kafka-on-centos-7

https://www.youtube.com/watch?v=ETCqVb2xQ1E&ab_channel=GKCodelabs

2. Run KafkaCsvProducer.java
3. Run App.java
4. List all kafka topics (not neccessary, just to see the topics)
/usr/lib/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181
5. Start comsumer (not neccesary, just to see the data sent from producer)
/usr/lib/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181 -topic BTD_FinalProject --from-beginning


