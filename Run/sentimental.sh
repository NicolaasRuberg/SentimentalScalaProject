sudo yum install -y hadoop-client
sudo yum install -y hadoop-hdfs
sudo yum install -y spark-core
sudo yum install -y java-1.8.0-openjdk


spark-submit --jars ./twitter4j-core.jar,./twitter4j-stream.jar,./spark-streaming.jar ./twitter_sentiment_analysis_2.11-0.1.jar
