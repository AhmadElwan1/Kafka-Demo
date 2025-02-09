using Confluent.Kafka;

namespace ProducerService.services
{
    public class KafkaProducerService
    {
        private readonly ProducerConfig _config; // Configuration for the Kafka producer

        public KafkaProducerService()
        {
            // Set up the Kafka broker (server) connection configuration
            _config = new ProducerConfig { BootstrapServers = "localhost:9092" };
        }

        /// <summary>
        /// Sends a message to a specified Kafka topic.
        /// </summary>
        /// <param name="topic">The name of the Kafka topic.</param>
        /// <param name="message">The message to be sent.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task SendMessageAsync(string topic, string message)
        {
            // Create a Kafka producer instance using the configuration
            using var producer = new ProducerBuilder<Null, string>(_config).Build();

            // Produce (send) a message asynchronously to the specified topic
            await producer.ProduceAsync(topic, new Message<Null, string> { Value = message });
        }
    }
}
