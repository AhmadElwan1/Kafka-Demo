using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;

namespace ProducerService.Controllers
{
    [Route("api/producer")]
    [ApiController]
    public class ProducerController : ControllerBase
    {
        private readonly string bootstrapServers = "localhost:9092"; // Kafka broker address
        private readonly string topic = "test-topic"; // Kafka topic where messages will be sent

        /// <summary>
        /// API endpoint that receives a message and sends it to Kafka.
        /// </summary>
        /// <param name="message">The message to be sent.</param>
        /// <returns>HTTP response with success or failure status.</returns>
        [HttpPost] // Defines an HTTP POST endpoint at "/api/producer"
        public async Task<IActionResult> SendMessage([FromBody] string message)
        {
            // Configure Kafka producer to connect to the specified broker
            var config = new ProducerConfig { BootstrapServers = bootstrapServers };

            // Create Kafka producer instance
            using var producer = new ProducerBuilder<Null, string>(config).Build();

            try
            {
                // Send message asynchronously to the Kafka topic
                var deliveryResult = await producer.ProduceAsync(topic, new Message<Null, string> { Value = message });

                // Return HTTP 200 (OK) with the message sent confirmation
                return Ok($"Message sent: {deliveryResult.Value}");
            }
            catch (Exception ex)
            {
                // Return HTTP 500 (Internal Server Error) if message sending fails
                return StatusCode(500, $"Error sending message: {ex.Message}");
            }
        }
    }
}
