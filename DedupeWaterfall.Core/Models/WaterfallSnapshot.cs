namespace DedupeWaterfall.Core.Models;

/// <summary>
/// Represents the frozen configuration of a waterfall at the time a lead run began.
/// </summary>
public class WaterfallSnapshot
{
    public long SnapshotId { get; set; }
    public long WaterfallId { get; set; }
    public string Name { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
    public List<WaterfallStep> Steps { get; set; } = [];
}
