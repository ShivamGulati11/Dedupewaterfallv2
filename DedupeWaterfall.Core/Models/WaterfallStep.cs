namespace DedupeWaterfall.Core.Models;

/// <summary>
/// A single lender entry within a waterfall snapshot, ordered by <see cref="StepOrder"/>.
/// </summary>
public class WaterfallStep
{
    public long StepId { get; set; }
    public long SnapshotId { get; set; }
    public long LenderId { get; set; }
    public string LenderName { get; set; } = string.Empty;
    public int StepOrder { get; set; }
    public string TriggerTopic { get; set; } = string.Empty;
}
