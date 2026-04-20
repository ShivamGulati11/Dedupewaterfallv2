using Dapper;
using DedupeWaterfall.Core.Enums;
using DedupeWaterfall.Core.Interfaces;
using DedupeWaterfall.Core.Models;
using DedupeWaterfall.Data.Infrastructure;

namespace DedupeWaterfall.Data.Repositories;

public class WaterfallRunRepository : IWaterfallRunRepository
{
    private readonly SqlConnectionFactory _connectionFactory;

    public WaterfallRunRepository(SqlConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task UpdateRunStatusAsync(
        long runId,
        WaterfallRunStatus status,
        int currentSequence,
        long? winningLenderId,
        CancellationToken ct)
    {
        const string sql = """
            UPDATE lead_waterfall_run
            SET    status            = @Status,
                   current_sequence  = @CurrentSequence,
                   winning_lender_id = @WinningLenderId,
                   completed_at      = CASE
                                         WHEN @Status IN ('Approved','RejectedAll','Error')
                                         THEN GETUTCDATE()
                                         ELSE NULL
                                       END
            WHERE  run_id = @RunId
            """;

        using var connection = _connectionFactory.CreateConnection();
        await connection.ExecuteAsync(
            new CommandDefinition(sql,
                new
                {
                    RunId = runId,
                    Status = status.ToString(),
                    CurrentSequence = currentSequence,
                    WinningLenderId = winningLenderId
                },
                cancellationToken: ct));
    }

    public async Task<LeadWaterfallRun?> GetRunAsync(long runId, CancellationToken ct)
    {
        const string sql = """
            SELECT run_id           AS RunId,
                   lead_id          AS LeadId,
                   base_id          AS BaseId,
                   current_sequence AS CurrentSequence,
                   status           AS Status,
                   winning_lender_id AS WinningLenderId,
                   started_at       AS StartedAt,
                   completed_at     AS CompletedAt
            FROM   lead_waterfall_run
            WHERE  run_id = @RunId
            """;

        using var connection = _connectionFactory.CreateConnection();
        return await connection.QuerySingleOrDefaultAsync<LeadWaterfallRun>(
            new CommandDefinition(sql, new { RunId = runId }, cancellationToken: ct));
    }
}
