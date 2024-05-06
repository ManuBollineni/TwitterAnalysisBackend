using System;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using TwitterAnalysis.Configs;
using Neo4j.Driver;
using TwitterAnalysis.Models;
using System.Text.Json;

namespace TwitterAnalysis.Controllers
{
	public class SentimentConsumerController : ControllerBase
	{
        string topic = "Sentiment"; // topic name

        [HttpGet("/SentimentDataConsumer")]
        public async Task<IActionResult> Index()
        {
            return Ok(await ConsumeSentimentdata());
        }

        private async Task<bool> ConsumeSentimentdata()
        {
            
            //var _driver = GraphDatabase.Driver("bolt://localhost:7687",
            //            AuthTokens.Basic("neo4j", "neo4jneo4j"));

            var config = KafkaConsumerConfig.GetConfig();
            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe(topic);
            while (true)
            {
                try
                {
                    var consumeResult = consumer.Consume();

                    Console.WriteLine($"Received message: {consumeResult.Message.Value}");

                    // Consume the message here
                    consumer.Commit(consumeResult);

                    Tweet tweetInfo = JsonSerializer.Deserialize<Tweet>(consumeResult.Message?.Value);


                    //update neo4j database using cypher query
                    if (tweetInfo != null)
                    {
                        var insertTweet = UpdateTweet(tweetInfo);
                    }

                    return await Task.FromResult(true);
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error consuming message: {e.Error.Reason}");
                }
                return await Task.FromResult(false);
            }
        }

        private async Task UpdateTweet(Tweet tweetInfo)
        {
            // Neo4j connection details
            var uri = "bolt://localhost:7687";
            var user = "neo4j";
            var password = "neo4jneo4j";

            // Cypher query to retrieve the tweet record by its identity
            var updateCypherQuery = @"
                                    MATCH (tweet:Tweet)
                                    WHERE ID(tweet) = $identity
                                    SET tweet.sentimentFeedback = $sentimentFeedback
                                    RETURN tweet";

            // Identity of the tweet record you want to retrieve
            var identity = tweetInfo.Id;

            // Create a driver instance
            using (var driver = GraphDatabase.Driver(uri, AuthTokens.Basic(user, password)))
            {
                // Establish a session with the database
                using (var session = driver.AsyncSession())
                {
                    // Run the Cypher query to update the sentimentFeedback property
                    var result = await session.RunAsync(updateCypherQuery, new { identity, sentimentFeedback = tweetInfo.SentimentFeedback });

                    // Check if the update was successful
                    if (await result.FetchAsync())
                    {
                        Console.WriteLine($"SentimentFeedback updated successfully for tweet with ID {identity}");
                    }
                    else
                    {
                        Console.WriteLine($"Failed to update SentimentFeedback for tweet with ID {identity}");
                    }



                }
            }
        }
    }
}

