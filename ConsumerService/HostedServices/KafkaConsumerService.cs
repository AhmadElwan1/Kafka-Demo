using Confluent.Kafka;
using Microsoft.Extensions.Hosting; 
using Microsoft.Extensions.Logging; 
using System;
using System.Threading;
using System.Threading.Tasks;

namespace ConsumerService.HostedServices
{
    /// <summary>
    /// KafkaConsumerService listens to a Kafka topic in the background and processes messages.
    /// It inherits from BackgroundService to run asynchronously.
    /// </summary>
    public class KafkaConsumerService : BackgroundService
    {
        private readonly ILogger<KafkaConsumerService> _logger; // Logger to track consumer activity
        private readonly string _bootstrapServers = "localhost:9092"; // Kafka broker address
        private readonly string _topic = "test-topic"; // Kafka topic to consume messages from
        private readonly string _groupId = "test-group"; // Consumer group ID
        private IConsumer<Ignore, string> _consumer; // Kafka consumer instance

        /// <summary>
        /// Constructor initializes the Kafka consumer with necessary configurations.
        /// </summary>
        /// <param name="logger">Logger instance for logging messages.</param>
        public KafkaConsumerService(ILogger<KafkaConsumerService> logger)
        {
            _logger = logger;

            // Kafka Consumer configuration
            var config = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers, // Kafka broker connection
                GroupId = _groupId, // Consumer group ID to track offsets
                AutoOffsetReset = AutoOffsetReset.Earliest // Read messages from the start if no offset is found
            };

            // Build the Kafka consumer instance
            _consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        }

        /// <summary>
        /// Runs the Kafka consumer asynchronously.
        /// </summary>
        /// <param name="stoppingToken">Cancellation token to stop the consumer.</param>
        /// <returns>A Task representing the asynchronous operation.</returns>
        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Run(() =>
            {
                // Register an event to log when the service receives a cancellation request
                stoppingToken.Register(() => _logger.LogInformation("Cancellation requested, stopping the consumer..."));

                try
                {
                    // Subscribe to the Kafka topic
                    _consumer.Subscribe(_topic);
                    _logger.LogInformation($"Kafka Consumer started and subscribed to topic: {_topic}");

                    // Keep consuming messages until cancellation is requested
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        try
                        {
                            // Poll for a message from the Kafka topic
                            var consumeResult = _consumer.Consume(stoppingToken);

                            // Check if the message is null or empty
                            if (consumeResult?.Message?.Value == null)
                            {
                                _logger.LogWarning("Received a null or empty message.");
                                continue; // Skip processing this message
                            }

                            // Log the received message
                            _logger.LogInformation($"Received message: {consumeResult.Message.Value}");

                            // Commit the offset to mark the message as processed
                            _consumer.Commit(consumeResult);
                        }
                        catch (ConsumeException ex)
                        {
                            // Log Kafka consumption errors and retry after a delay
                            _logger.LogError($"Consume error: {ex.Error.Reason}");
                            Task.Delay(TimeSpan.FromSeconds(5), stoppingToken).Wait(stoppingToken);
                        }
                        catch (KafkaException kEx)
                        {
                            // Handle Kafka-specific errors
                            _logger.LogError($"Kafka error: {kEx.Error.Reason}");
                            Task.Delay(TimeSpan.FromSeconds(5), stoppingToken).Wait(stoppingToken);
                        }
                        catch (ObjectDisposedException ex)
                        {
                            // Handle consumer disposal (happens if service is shutting down)
                            _logger.LogError($"Consumer has been disposed: {ex.Message}");
                            break; // Exit loop
                        }
                        catch (Exception ex)
                        {
                            // Catch any unexpected errors and retry after a delay
                            _logger.LogError($"Unexpected error: {ex.Message}");
                            Task.Delay(TimeSpan.FromSeconds(5), stoppingToken).Wait(stoppingToken);
                        }
                    }
                }
                finally
                {
                    // Close the Kafka consumer when stopping
                    _consumer.Close();
                    _logger.LogInformation("Consumer closed gracefully.");
                }
            }, stoppingToken);
        }

        /// <summary>
        /// Disposes of the Kafka consumer when the service stops.
        /// </summary>
        public override void Dispose()
        {
            _consumer?.Dispose();
            base.Dispose();
        }
    }
}
