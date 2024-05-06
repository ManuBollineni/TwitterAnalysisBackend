using System;
namespace TwitterAnalysis.Services
{
	public class SentimentAnalysisService
	{
        public string SentimentAnalysisSentimentAnalyzer(string txt)
        {
            // Assuming SentimentAnalyzer is a class with a static method Predict
            var sentiment = SentimentAnalyzer.Sentiments.Predict(txt);

            if (sentiment.Score > 0)
            {
                return "Positive";
            }
            else if (sentiment.Score == 0)
            {
                return "Neutral";
            }
            else
            {
                return "Negative";
            }
        }
    }
}

