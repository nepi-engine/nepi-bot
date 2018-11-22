# Kafka Installation Procedure

This provides a procedure for installing Apache Kafka on an Ubuntu Linux EC2 instance.

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


### Starting Zookeeper

TBD

### Starting Kafka

TBD

### Prepare Kafka as a Docker Image

TBD
