namespace DedupeWaterfall.Core.Models;

public class LeadWaterfallRun
{
    public long RunId { get; set; }
    public long LeadId { get; set; }
    public long BaseId { get; set; }
    public int CurrentSequence { get; set; }
    public string Status { get; set; } = string.Empty;
    public int? WinningLenderId { get; set; }
    public DateTime StartedAt { get; set; }
    public DateTime? CompletedAt { get; set; }
}
