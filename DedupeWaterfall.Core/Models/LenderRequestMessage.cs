using DedupeWaterfall.Core.Interfaces;

namespace DedupeWaterfall.Core.Models;

public class LenderRequestMessage : ILeadMessage
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
    public int LenderId { get; set; }
    public string LenderCode { get; set; } = string.Empty;
    public int SequenceOrder { get; set; }
    public List<string> SkipLenders { get; set; } = new();
    public Dictionary<string, string> CachedStatuses { get; set; } = new();
    public DateTime Timestamp { get; set; }
    public Guid CorrelationId { get; set; }
}
