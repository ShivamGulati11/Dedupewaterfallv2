namespace DedupeWaterfall.Core.Contracts;

/// <summary>
/// Published to a lender-specific Kafka topic to trigger evaluation of a lead.
/// </summary>
public class LenderTriggerMessage
{
    public Guid MessageId { get; set; }
    public long RunId { get; set; }
    public long LeadId { get; set; }
    public long LenderId { get; set; }
    public long BaseId { get; set; }
    public string GuserId { get; set; } = string.Empty;
    public string Mobile { get; set; } = string.Empty;
    public string Pan { get; set; } = string.Empty;
    public string FullName { get; set; } = string.Empty;
    public DateTime TriggeredAt { get; set; }
}
