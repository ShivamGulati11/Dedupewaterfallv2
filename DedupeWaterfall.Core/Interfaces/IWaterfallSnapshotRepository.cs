using DedupeWaterfall.Core.Models;

namespace DedupeWaterfall.Core.Interfaces;

/// <summary>
/// Retrieves a frozen waterfall snapshot (with ordered steps) from persistent storage.
/// </summary>
public interface IWaterfallSnapshotRepository
{
    Task<WaterfallSnapshot?> GetByIdAsync(long snapshotId, CancellationToken ct = default);
}
