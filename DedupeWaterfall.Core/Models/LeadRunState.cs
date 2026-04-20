using DedupeWaterfall.Core.Enums;

namespace DedupeWaterfall.Core.Models;

/// <summary>
/// Tracks the current state of a lead run as it progresses through the waterfall.
/// </summary>
public class LeadRunState
{
    public long RunId { get; set; }
    public long LeadId { get; set; }
    public long BaseId { get; set; }
    public long SnapshotId { get; set; }
    public LeadStatus Status { get; set; }
    public long? WinnerLenderId { get; set; }
    public int CurrentStepOrder { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }
}
