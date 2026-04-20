using DedupeWaterfall.Core.Enums;
using DedupeWaterfall.Core.Models;

namespace DedupeWaterfall.Core.Interfaces;

/// <summary>
/// Persists and retrieves the mutable run-state of a lead as it traverses the waterfall.
/// </summary>
public interface ILeadRunStateRepository
{
    Task<LeadRunState?> GetByRunIdAsync(long runId, CancellationToken ct = default);
    Task UpsertAsync(LeadRunState state, CancellationToken ct = default);
    Task UpdateStatusAsync(long runId, LeadStatus status, long? winnerLenderId = null, CancellationToken ct = default);
}
