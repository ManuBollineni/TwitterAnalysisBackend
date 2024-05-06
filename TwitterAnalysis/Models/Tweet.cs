using System;
namespace TwitterAnalysis.Models
{
    public class Tweet
    {
        public string[] Labels { get; set; }
        public int Favorites { get; set; }
        public string ImportMethod { get; set; }
        public string IdStr { get; set; }
        public DateTime CreatedAt { get; set; }
        public string Text { get; set; }
        public long Id { get; set; }
        public string ElementId { get; set; }

        public string? SentimentFeedback { get; set; }
    }
}

