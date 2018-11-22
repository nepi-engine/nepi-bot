# Kafka Installation Procedure

This provides a procedure for installing Apache Kafka on an Amazon Linux EC2 instance.

### Install Java 8

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

Imagme: __Image Snapshot: 1\_Kafka\_AfterJava8__


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

Image: __Image Snapshot: 2\_Kafka\_AfterKafkaExtract__


### Starting Zookeeper

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

Start Zookeeper:

````
cd kafka_2.12-2.1.0
nohup bin/zookeeper-server-start.sh config/zookeeper.properties > ~/zookeeper-logs &
````

### Starting Kafka

````
cd kafka_2.12-2.1.0
nohup bin/kafka-server-start.sh config/server.properties > ~/kafka-logs &
````

### Access Kafka-Server via Kafka-Scala or Kafka-Java API

TBD

### Stopping Kafka & Zookeeper

````
cd kafka_2.12-0.10.2.0
bin/kafka-server-stop.sh
bin/zookeeper-server-stop.sh
````

### Prepare Kafka as a Docker Image

TBD

### Source

This procedure used the following:

* [Installing and Running Kafka on an AWS Instance](https://dzone.com/articles/installing-and-running-kafka-on-an-aws-instance)