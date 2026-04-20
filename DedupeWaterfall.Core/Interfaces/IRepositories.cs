using DedupeWaterfall.Core.Models;
using DedupeWaterfall.Core.Enums;

namespace DedupeWaterfall.Core.Interfaces;

public interface IWaterfallConfigRepository
{
    /// <summary>
    /// Returns all lenders in this snapshot ordered by sequence_order ascending.
    /// </summary>
    Task<List<WaterfallConfigSnapshot>> GetSnapshotAsync(long snapshotId, CancellationToken ct);
}

public interface IWaterfallRunRepository
{
    Task UpdateRunStatusAsync(long runId, WaterfallRunStatus status,
        int currentSequence, long? winningLenderId, CancellationToken ct);

    Task<LeadWaterfallRun?> GetRunAsync(long runId, CancellationToken ct);
}

public interface IDedupeHitRepository
{
    /// <summary>
    /// Inserts a dedupe hit with status=Pending and returns the new hit_id.
    /// </summary>
    Task<long> InsertHitAsync(long runId, long leadId, int lenderId,
        int sequenceOrder, CancellationToken ct);
}

public interface IEventLogRepository
{
    /// <summary>
    /// Buffers an event into Redis — does NOT write to SQL directly.
    /// </summary>
    Task BufferEventAsync(long runId, long leadId, int? lenderId,
        string eventType, object? payload, CancellationToken ct);
}

public interface IKafkaProducer
{
    Task ProduceAsync<T>(string topic, string key, T message, CancellationToken ct);
}
