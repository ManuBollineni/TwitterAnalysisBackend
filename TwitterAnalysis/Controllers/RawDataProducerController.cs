using System;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;
using System.Text.Json;
using TwitterAnalysis.Models;
using Confluent.Kafka;

namespace TwitterAnalysis.Controllers
{
    [ApiController]
    [Route("api")]
    public class RawDataProducerController : ControllerBase
    {
        private readonly string
        bootstrapServers = "localhost:9092";
        private readonly string topic = "Raw";

        [HttpPost("/RawdataProducer")]
        public async Task<IActionResult> Post([FromBody] Tweet tweetInfo)
        {
            string message = JsonSerializer.Serialize(tweetInfo);
            return Ok(await SendRawdataRequest(topic, message));
        }

        private async Task<bool> SendRawdataRequest(string topic, string message)
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

