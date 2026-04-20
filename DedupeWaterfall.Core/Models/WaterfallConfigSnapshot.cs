namespace DedupeWaterfall.Core.Models;

public class WaterfallConfigSnapshot
{
    public long SnapshotId { get; set; }
    public long BaseId { get; set; }
    public int LenderId { get; set; }
    public string LenderCode { get; set; } = string.Empty;
    public int SequenceOrder { get; set; }
}
