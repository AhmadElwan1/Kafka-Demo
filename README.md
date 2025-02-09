# Kafka .NET 9 Demo

This is a simple **Kafka demo** built with **.NET 9**, featuring two microservices:
- **Producer Service**: Sends messages to Kafka.
- **Consumer Service**: Listens for messages from Kafka.

## 🛠️ Prerequisites
Ensure you have the following installed:
- **.NET 9 SDK**
- **Kafka & Zookeeper** (Installed on Ubuntu)
- **VS Code** (or any preferred editor)

## 🚀 Getting Started

### 1️⃣ Start Zookeeper & Kafka
Open a terminal and run:
```sh
bin/zookeeper-server-start.sh config/zookeeper.properties
```
In a new terminal, start Kafka:
```sh
bin/kafka-server-start.sh config/server.properties
```
Create a Kafka topic:
```sh
bin/kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Run Producer and Consumer:
```sh
dotnet run
```

Send test Message:
```sh
curl -X POST http://localhost:5051/api/producer -H "Content-Type: application/json" -d '"Hello, Kafka!"'
```