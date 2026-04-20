namespace DedupeWaterfall.Core.Kafka;

public static class WaterfallEventType
{
    public const string LeadQueued = "LeadQueued";
    public const string DedupeHitInitiated = "DedupeHitInitiated";
    public const string DedupeApproved = "DedupeApproved";
    public const string DedupeRejected = "DedupeRejected";
    public const string WaterfallComplete = "WaterfallComplete";
    public const string Skipped5DayWindow = "Skipped5DayWindow";
    public const string Error = "Error";
}
