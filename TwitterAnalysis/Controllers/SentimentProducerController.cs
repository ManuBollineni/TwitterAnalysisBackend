using System;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;
using TwitterAnalysis.Models;
using System.Text.Json;

namespace TwitterAnalysis.Controllers
{
	public class SentimentProducerController : ControllerBase
	{
        private readonly string
        bootstrapServers = "localhost:9092";
        private readonly string topic = "Sentiment";

        [HttpPost("/SentimentdataProducer")]
        public async Task<IActionResult> Post([FromBody] Tweet tweetInfo)
        {
            string message = JsonSerializer.Serialize(tweetInfo);
            return Ok(await SendOrderRequest(topic, message));
        }

        public async Task<bool> SendOrderRequest(string topic, string message)
        {
            ProducerConfig config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                SaslMechanism = SaslMechanism.Plain
            };

            try
            {
                using (var producer = new ProducerBuilder<string, string>(config).Build())
                {
                    var result = await producer.ProduceAsync(topic, new Message<string, string> { Key = null, Value = message });
                    Debug.WriteLine($"Delivery Timestamp:{result.Timestamp.UtcDateTime}");
                    return await Task.FromResult(true);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error occured: {ex.Message}");
            }

            return await Task.FromResult(false);
        }
    }
}

