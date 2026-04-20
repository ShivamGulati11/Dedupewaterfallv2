namespace DedupeWaterfall.Orchestrator.Options;

public class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:9092";
    public string ConsumerGroupId { get; set; } = "dedupe-orchestrator";
    public string LeadsQueuedTopic { get; set; } = "dedupe.leads.queued";
    public string LenderResultTopic { get; set; } = "dedupe.lender.result";
    public string LeadsWonTopic { get; set; } = "dedupe.leads.won";
    public string LeadsExhaustedTopic { get; set; } = "dedupe.leads.exhausted";
    public string SecurityProtocol { get; set; } = "Plaintext";
    public string? SaslMechanism { get; set; }
    public string? SaslUsername { get; set; }
    public string? SaslPassword { get; set; }
}
