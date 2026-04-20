using DedupeWaterfall.Core.Contracts;

namespace DedupeWaterfall.Core.Interfaces;

/// <summary>
/// Core waterfall routing logic: decides whether to trigger the next lender,
/// declare a winner, or mark the lead as exhausted.
/// </summary>
public interface IOrchestratorService
{
    /// <summary>
    /// Called when a new lead enters the system. Triggers the first eligible
    /// lender from the frozen snapshot or exhausts the lead immediately if none remain.
    /// </summary>
    Task HandleLeadQueuedAsync(LeadQueuedMessage message, CancellationToken ct = default);

    /// <summary>
    /// Called when a lender posts its decision. Declares a winner on acceptance,
    /// or triggers the next eligible lender on rejection.
    /// </summary>
    Task HandleLenderResultAsync(LenderResultMessage message, CancellationToken ct = default);
}
