namespace DedupeWaterfall.Core.Contracts;

/// <summary>
/// Published to <c>dedupe.leads.won</c> when a lender accepts the lead.
/// </summary>
public class LeadWonMessage
{
    public Guid MessageId { get; set; }
    public long RunId { get; set; }
    public long LeadId { get; set; }
    public long WinnerLenderId { get; set; }
    public string WinnerLenderName { get; set; } = string.Empty;
    public int WinningStepOrder { get; set; }
    public DateTime WonAt { get; set; }
}
