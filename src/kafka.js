const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'app-id',
  brokers: ['localhost:9092']
})

const producer = kafka.producer()

export emitMessage = (topic, message) => {
    async () => {
        await producer.connect()
        await producer.send({
          topic: topic,
          messages: [
            message
          ],
        })
        await producer.disconnect()
      }
}
