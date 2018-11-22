# NEPI-API #

The NEPI-API is responsible for:

* Handling all business logic required by user interface actions & events that require access to back-end resources (e.g. Kafka topics, persisted data such as file or database).
* Sending status & data provided by NEPI-Server to NEPI-Portal.
* Sending user initiated data (e.g. updated configuratoin) from NEPI-Portal to NEPI-Server.

The bi-directional communication between NEPI-Server and NEPI-Portal is faciliated by an Apache Kafka cluster.  Thus, the NEPI-API subscribes to topics published by the Numurus Floats and the Numurus Floats can receive data (via Iridium) to topics published by the NEPI-API.

## NEPI-API Assets

The NEPI-API implementation details ... TBD

## Development Apache Kafka Instance

A development Apache Kafka instance has been created to provide the team a sandbox for developing the patterns needed for implementing bi-directional communication between NEPI-API and NEPI-Server.

|  Key  |  Value  |
|---|---|
|  Region | US West (Oregon) |
|  AMI Name |  Ubuntu Server 18.04 LTS (HVM), SSD Volume Type |
|  AMI Description |  Ubuntu Server 18.04 LTS (HVM),EBS General Purpose (SSD) Volume Type. Support available from Canonical (http://www.ubuntu.com/cloud/services). |
| Type | t2.micro (Free tier eligible) |
| vCPUs | 1 |
| Memory | 2 GB |
| Instance Storage | EBS only |
| IPv6 Support | Yes |
| VPC | vpc-b5a7acd |
| Storage | Root, 8 GB, General Purpose SSD (gp2), IOPS: 100/3000
| Security Group | nepi-kafka-security-group | 
| Key pair | nepi-kafka | 
| Elastic IP | 52.26.24.209 |

__Connecting to the Development Kafka Instance__

Connecting to the Kafka Development Instance is available through SSH for server administration.  To connect, you must have access to the key pair (nepi-api.pem) and a custom rule needs to be created for your developer machine in the security group.

The Kafka Development Instance will be stopped after business hours or when not in use.

The Administration notes for instlaling Apache Kafka on an Amazon EC2 Linux can be found [here](/docs/kafka-install.md)

## Contact

* This repository is maintained by [Clint Cabanero](clint.cabanero@critigen.com) and [Tim Kearns](tkearns@numurus.com)