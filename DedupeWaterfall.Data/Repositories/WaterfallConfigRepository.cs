using Dapper;
using DedupeWaterfall.Core.Interfaces;
using DedupeWaterfall.Core.Models;
using DedupeWaterfall.Data.Infrastructure;

namespace DedupeWaterfall.Data.Repositories;

public class WaterfallConfigRepository : IWaterfallConfigRepository
{
    private readonly SqlConnectionFactory _connectionFactory;

    public WaterfallConfigRepository(SqlConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task<List<WaterfallConfigSnapshot>> GetSnapshotAsync(
        long snapshotId, CancellationToken ct)
    {
        const string sql = """
            SELECT snapshot_id  AS SnapshotId,
                   base_id      AS BaseId,
                   lender_id    AS LenderId,
                   lender_code  AS LenderCode,
                   sequence_order AS SequenceOrder
            FROM   waterfall_config_snapshot
            WHERE  snapshot_id = @SnapshotId
            ORDER  BY sequence_order ASC
            """;

        using var connection = _connectionFactory.CreateConnection();
        var results = await connection.QueryAsync<WaterfallConfigSnapshot>(
            new CommandDefinition(sql, new { SnapshotId = snapshotId },
                cancellationToken: ct));

        return results.ToList();
    }
}
