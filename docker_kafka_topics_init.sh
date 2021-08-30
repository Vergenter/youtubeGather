# run in kafka brooker
kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 3 --topic new_channels 
kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 3 --topic new_videos
kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 3 --topic video_comments
kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 3 --topic comment_replies
kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 3 --topic updates