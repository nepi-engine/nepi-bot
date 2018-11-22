# Kafka Installation Procedure

This provides a procedure for installing Apache Kafka on an Amazon Linux EC2 instance.

### Install Java 8

Connect to the Kafka EC2 instance:

````
ssh -i nepi-kafka.pem ec2-user@52.39.250.140
````

We will be using the Kafka binary for Scala 2.12 - which requires Java 8.

Confirm Java 8 is NOT installed with:

````
java -version // results in'Command 'java' not found
````

Download TPM package of Oracle JDK with:

````
wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u141-b15/336fa29ff2bb4ef291e347e091f7f4a7/jdk-8u141-linux-x64.rpm
````

Then install JDK 8 with:

````
sudo yum install -y jdk-8u141-linux-x64.rpm
````

Confirm Java 8 is installed with:

````
java -version

java version "1.8.0_141"
Java(TM) SE Runtime Environment (build 1.8.0_141-b15)
Java HotSpot(TM) 64-Bit Server VM (build 25.141-b15, mixed mode)
````

Edit .bashrc

````
nano .bashrc

````

Add the following:

````
export JAVA_HOME="/usr/java/jdk1.8.0_141/jre"

PATH=$JAVA_HOME/bin:$PATH
````

Then source the .bashrc file:

````
source .bashrc

````

#### Download and Extract Kafka

Download:

````
wget https://www-eu.apache.org/dist/kafka/2.1.0/kafka_2.12-2.1.0.tgz 
````

Extract: 

````
tar -xzf kafka_2.12-2.1.0.tgz
````

Remove Zip:

````
rm kafka_2.12-2.1.0.tgz
````

### Configure Zookeeper

We start a single-node Zookeeper instance using the convenience script packaged with Kafka. 

Set the KAFKA\_HEAP_OPTS environment variable:

````
nano .bashrc
````

Then set the following environmental variable for 50% of our system's RAM (which is 1GB):

````
export KAFKA_HEAP_OPTS="-Xmx500M -Xms500M"
````

Then soure the .baschrc with:

````
source .bashrc
````

### Start Zookeeper

Before starting Kafka, you need to first start Zookeeper (manages all the Brokers for a Topic)

Start Zookeeper:

````
cd kafka_2.12-2.1.0

bin/zookeeper-server-start.sh config/zookeeper.properties
````

Everything is working if you see the following at the end:

````
INFO binding to port 0.0.0.0/0.0.0.0:2181 (org.apache.zookeeper.server.NIOServerCnxnFactory)
````

Keep Zookeeper running by keeping this Terminal window open.

### Starting Kafka

With Zooker started, you can now start Kafka.  Command + Tab to open a new Terminal Window then start Kafka with:

````
cd kafka_2.12-2.1.0

bin/kafka-server-start.sh config/server.properties
````

Everything is working if you see the following at the end:

````
INFO [KafkaServer id=0] started (kafka.server.KafkaServer)
````

### Using the Kafka CLI

To test (or learn how to use) Kafka, here are some basic commands using the Kafka CLI while SSH-connected to the instance.

Create a Topic:

````
bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic first_topic --create --partitions 3 --replication-factor 1
````

List Topics:

````
bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --list
````

Get Topic Info:

````
bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic first_topic --describe
````

A Producer publishes messages it to a topic:

````
bin/kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic first_topic

> test 1
> test 2
> test 3
````

Consumer subscribes to messages from a topic:

````
bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic

> test 1
> test 2
> test 3
````

### Access Kafka-Server via Kafka-Scala or Kafka-Java API

TBD

### Stopping Kafka & Zookeeper

````
cd kafka_2.12-0.10.2.0
bin/kafka-server-stop.sh
bin/zookeeper-server-stop.sh
````

### Prepare an Amazon EC2 Image 

The basic install of Java 8 and Kafka has been encpsulated in an Amazon Machine Image: ami-020d098ef68a3f1ad

This can be used to launch Kafka EC2 image without the need to do all of teh above install steps.

Note, if the amount of Memory is changed, you may want to change the memory allocated to Kafka in .bashrc as this image assigns 500 MB for a 1GB t2.small instance.

````
export KAFKA_HEAP_OPTS="-Xmx500M -Xms500M"
```` 


### Prepare Kafka as a Docker Image

TBD
