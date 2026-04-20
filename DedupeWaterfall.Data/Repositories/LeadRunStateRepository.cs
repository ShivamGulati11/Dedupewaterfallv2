using Dapper;
using DedupeWaterfall.Core.Enums;
using DedupeWaterfall.Core.Interfaces;
using DedupeWaterfall.Core.Models;
using DedupeWaterfall.Data.Infrastructure;

namespace DedupeWaterfall.Data.Repositories;

public sealed class LeadRunStateRepository : ILeadRunStateRepository
{
    private readonly SqlConnectionFactory _factory;

    public LeadRunStateRepository(SqlConnectionFactory factory)
    {
        _factory = factory;
    }

    public async Task<LeadRunState?> GetByRunIdAsync(long runId, CancellationToken ct = default)
    {
        const string sql = @"
            SELECT RunId, LeadId, BaseId, SnapshotId, Status, WinnerLenderId,
                   CurrentStepOrder, CreatedAt, UpdatedAt
            FROM   LeadRunStates
            WHERE  RunId = @RunId";

        using var conn = _factory.Create();
        return await conn.QuerySingleOrDefaultAsync<LeadRunState>(
            new CommandDefinition(sql, new { RunId = runId }, cancellationToken: ct));
    }

    public async Task UpsertAsync(LeadRunState state, CancellationToken ct = default)
    {
        const string sql = @"
            MERGE LeadRunStates WITH (HOLDLOCK) AS target
            USING (SELECT @RunId AS RunId) AS source ON target.RunId = source.RunId
            WHEN MATCHED THEN
                UPDATE SET LeadId           = @LeadId,
                           BaseId           = @BaseId,
                           SnapshotId       = @SnapshotId,
                           Status           = @Status,
                           WinnerLenderId   = @WinnerLenderId,
                           CurrentStepOrder = @CurrentStepOrder,
                           UpdatedAt        = @UpdatedAt
            WHEN NOT MATCHED THEN
                INSERT (RunId, LeadId, BaseId, SnapshotId, Status, WinnerLenderId,
                        CurrentStepOrder, CreatedAt, UpdatedAt)
                VALUES (@RunId, @LeadId, @BaseId, @SnapshotId, @Status, @WinnerLenderId,
                        @CurrentStepOrder, @CreatedAt, @UpdatedAt);";

        using var conn = _factory.Create();
        await conn.ExecuteAsync(new CommandDefinition(sql, state, cancellationToken: ct));
    }

    public async Task UpdateStatusAsync(
        long runId,
        LeadStatus status,
        long? winnerLenderId = null,
        CancellationToken ct = default)
    {
        const string sql = @"
            UPDATE LeadRunStates
            SET    Status         = @Status,
                   WinnerLenderId = @WinnerLenderId,
                   UpdatedAt      = @UpdatedAt
            WHERE  RunId = @RunId";

        using var conn = _factory.Create();
        await conn.ExecuteAsync(new CommandDefinition(sql,
            new { RunId = runId, Status = status, WinnerLenderId = winnerLenderId, UpdatedAt = DateTime.UtcNow },
            cancellationToken: ct));
    }
}
