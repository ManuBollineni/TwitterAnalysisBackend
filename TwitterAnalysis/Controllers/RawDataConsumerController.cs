using System;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using TwitterAnalysis.Configs;
using TwitterAnalysis.Models;
using TwitterAnalysis.Services;

namespace TwitterAnalysis.Controllers
{
    [ApiController]
    [Route("api")]
    public class RawDataConsumerController : ControllerBase
	{
        private readonly SentimentAnalysisService _sentimentAnalysisService;
        private readonly SentimentProducerController _sentimentProducerController;

        string rawTopic = "Raw"; // Replace with your topic name
        string sentimentTopic = "Sentiment";
        public RawDataConsumerController(SentimentAnalysisService sentimentAnalysisService, SentimentProducerController sentimentProducerController)
        {
            _sentimentAnalysisService = sentimentAnalysisService; // consuming services form pipeline
            _sentimentProducerController = sentimentProducerController;
        }

        [HttpGet("/GetRawdataConsumer")]
        public async Task<IActionResult> Index()
        {
            return Ok(await ConsumeRawdata());
        }

        private async Task<bool> ConsumeRawdata()
        {
            var config = KafkaConsumerConfig.GetConfig();
            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe(rawTopic);
            while (true)
            {
                try
                {
                    var consumeResult = consumer.Consume();
                    Console.WriteLine($"Received message: {consumeResult.Message.Value}");

                    // Consume the message here
                    consumer.Commit(consumeResult);

                    string _sentiment;
                    Tweet tweetInfo = JsonSerializer.Deserialize<Tweet>(consumeResult.Message.Value);

                    // Perform sentiment Analysis Here
                    if (tweetInfo != null && tweetInfo.Text != "")
                    {
                        _sentiment = _sentimentAnalysisService.SentimentAnalysisSentimentAnalyzer(tweetInfo.Text);
                        tweetInfo.SentimentFeedback = _sentiment;
                    }


                    // Produce Msg to topic: Sentiment
                    string message = JsonSerializer.Serialize(tweetInfo);
                    bool result = await _sentimentProducerController.SendOrderRequest(sentimentTopic, message);



                    return await Task.FromResult(true);
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error consuming message: {e.Error.Reason}");
                }
                return await Task.FromResult(false);
            }
        }
    }
}

