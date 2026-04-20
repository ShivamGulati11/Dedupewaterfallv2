namespace DedupeWaterfall.Orchestrator.Options;

public class OrchestratorOptions
{
    /// <summary>Maximum number of delivery attempts before routing to DLQ.</summary>
    public int MaxRetryCount { get; set; } = 3;

    /// <summary>Milliseconds to wait between retry attempts.</summary>
    public int RetryDelayMs { get; set; } = 1000;
}
