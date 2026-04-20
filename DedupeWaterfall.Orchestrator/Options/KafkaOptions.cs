namespace DedupeWaterfall.Orchestrator.Options;

public class KafkaOptions
{
    public string BootstrapServers { get; set; } = string.Empty;
    public string ClientId { get; set; } = string.Empty;
    public ConsumerGroupOptions ConsumerGroups { get; set; } = new();
}

public class ConsumerGroupOptions
{
    public string LeadQueued { get; set; } = "waterfall-orchestrator-group";
    public string LenderResult { get; set; } = "waterfall-result-group";
}
