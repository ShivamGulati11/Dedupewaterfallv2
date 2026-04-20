namespace DedupeWaterfall.Core.Contracts;

/// <summary>
/// Published to <c>dedupe.leads.exhausted</c> when all lenders in the waterfall
/// have rejected the lead.
/// </summary>
public class LeadExhaustedMessage
{
    public Guid MessageId { get; set; }
    public long RunId { get; set; }
    public long LeadId { get; set; }
    public int TotalStepsAttempted { get; set; }
    public DateTime ExhaustedAt { get; set; }
}
