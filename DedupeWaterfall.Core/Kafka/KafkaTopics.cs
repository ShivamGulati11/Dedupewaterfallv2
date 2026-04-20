namespace DedupeWaterfall.Core.Kafka;

public static class KafkaTopics
{
    public const string LeadsQueued = "dedupe.leads.queued";
    public const string LenderResult = "dedupe.lender.result";
    public const string WaterfallEvents = "dedupe.waterfall.events";

    public static string LenderRequest(string lenderCode) =>
        $"dedupe.lender.{lenderCode.ToUpper()}.request";
}
