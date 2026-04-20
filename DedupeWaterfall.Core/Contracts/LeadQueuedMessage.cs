using DedupeWaterfall.Core.Enums;

namespace DedupeWaterfall.Core.Contracts;

/// <summary>
/// Published to <c>dedupe.leads.queued</c> when a new lead enters the system.
/// </summary>
public class LeadQueuedMessage
{
    public Guid MessageId { get; set; }
    public long BaseId { get; set; }
    public long RunId { get; set; }
    public long LeadId { get; set; }
    public string GuserId { get; set; } = string.Empty;
    public string Mobile { get; set; } = string.Empty;
    public string Pan { get; set; } = string.Empty;
    public string FullName { get; set; } = string.Empty;
    public long SnapshotId { get; set; }

    /// <summary>Lender IDs that must be skipped for this lead run.</summary>
    public List<long> SkipLenders { get; set; } = [];

    /// <summary>
    /// Pre-computed statuses keyed by LenderId, used to fast-forward through
    /// already-decided lenders without re-contacting them.
    /// </summary>
    public Dictionary<long, LenderStatus> CachedStatuses { get; set; } = [];
}
