namespace DedupeWaterfall.Core.Interfaces;

/// <summary>
/// Common fields shared by all lead-related Kafka messages,
/// enabling polymorphic routing in OrchestratorService.
/// </summary>
public interface ILeadMessage
{
    long RunId { get; }
    long LeadId { get; }
    long BaseId { get; }
    string GuserId { get; }
    string Mobile { get; }
    string Pan { get; }
    string FullName { get; }
    long SnapshotId { get; }
    List<string> SkipLenders { get; }
    Dictionary<string, string> CachedStatuses { get; }
    Guid CorrelationId { get; }
}
