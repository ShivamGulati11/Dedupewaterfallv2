using DedupeWaterfall.Core.Enums;

namespace DedupeWaterfall.Core.Contracts;

/// <summary>
/// Published to <c>dedupe.lender.result</c> when a lender responds to a trigger.
/// </summary>
public class LenderResultMessage
{
    public Guid MessageId { get; set; }
    public long RunId { get; set; }
    public long LeadId { get; set; }
    public long LenderId { get; set; }
    public LenderStatus Status { get; set; }

    /// <summary>Human-readable reason for acceptance or rejection.</summary>
    public string? Reason { get; set; }

    public DateTime ResultAt { get; set; }
}
