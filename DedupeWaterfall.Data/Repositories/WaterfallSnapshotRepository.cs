using Dapper;
using DedupeWaterfall.Core.Interfaces;
using DedupeWaterfall.Core.Models;
using DedupeWaterfall.Data.Infrastructure;

namespace DedupeWaterfall.Data.Repositories;

public sealed class WaterfallSnapshotRepository : IWaterfallSnapshotRepository
{
    private readonly SqlConnectionFactory _factory;

    public WaterfallSnapshotRepository(SqlConnectionFactory factory)
    {
        _factory = factory;
    }

    public async Task<WaterfallSnapshot?> GetByIdAsync(long snapshotId, CancellationToken ct = default)
    {
        const string snapshotSql = @"
            SELECT SnapshotId, WaterfallId, Name, CreatedAt
            FROM   WaterfallSnapshots
            WHERE  SnapshotId = @SnapshotId";

        const string stepsSql = @"
            SELECT StepId, SnapshotId, LenderId, LenderName, StepOrder, TriggerTopic
            FROM   WaterfallSteps
            WHERE  SnapshotId = @SnapshotId
            ORDER  BY StepOrder ASC";

        using var conn = _factory.Create();

        var snapshot = await conn.QuerySingleOrDefaultAsync<WaterfallSnapshot>(
            new CommandDefinition(snapshotSql, new { SnapshotId = snapshotId }, cancellationToken: ct));

        if (snapshot is null)
            return null;

        var steps = await conn.QueryAsync<WaterfallStep>(
            new CommandDefinition(stepsSql, new { SnapshotId = snapshotId }, cancellationToken: ct));

        snapshot.Steps = steps.ToList();
        return snapshot;
    }
}
