using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace ConsumerService.HostedServices
{
    public class KafkaConsumerService : BackgroundService // Inherits from BackgroundService to run in the background
    {
        private readonly ILogger<KafkaConsumerService> _logger; // Logger for tracking consumer activity
        private readonly string bootstrapServers = "localhost:9092"; // Kafka broker address
        private readonly string topic = "test-topic"; // Kafka topic to consume messages from
        private readonly string groupId = "test-group"; // Consumer group ID

        /// <summary>
        /// Constructor that initializes the logger.
        /// </summary>
        /// <param name="logger">Logger for tracking message consumption</param>
        public KafkaConsumerService(ILogger<KafkaConsumerService> logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// Executes the Kafka consumer asynchronously.
        /// </summary>
        /// <param name="stoppingToken">Cancellation token to stop the service</param>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // Kafka Consumer configuration
            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers, // Connects to Kafka server
                GroupId = groupId, // Identifies this consumer as part of a group
                AutoOffsetReset = AutoOffsetReset.Earliest // Reads messages from the beginning if no offset is found
            };

            // Create a Kafka consumer instance
            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

            // Subscribe to the Kafka topic
            consumer.Subscribe(topic);

            _logger.LogInformation("Kafka Consumer started and subscribed to topic: " + topic);

            // Infinite loop to listen for new messages
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // Consume a message from the Kafka topic
                    var consumeResult = consumer.Consume(stoppingToken);

                    // Log the received message
                    _logger.LogInformation($"Received message: {consumeResult.Message.Value}");
                }
                catch (ConsumeException ex)
                {
                    // Log any errors encountered while consuming messages
                    _logger.LogError($"Consume Exception: {ex.Error.Reason}");
                }
            }
        }
    }
}