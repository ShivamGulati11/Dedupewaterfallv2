namespace DedupeWaterfall.Orchestrator.Options;

public class OrchestratorOptions
{
    public const string SectionName = "Orchestrator";

    /// <summary>
    /// Maximum number of messages processed concurrently within a single worker.
    /// </summary>
    public int MaxConcurrency { get; set; } = 10;

    /// <summary>
    /// How long (ms) the Kafka consumer blocks waiting for a new message before looping.
    /// </summary>
    public int ConsumePollTimeoutMs { get; set; } = 200;
}
