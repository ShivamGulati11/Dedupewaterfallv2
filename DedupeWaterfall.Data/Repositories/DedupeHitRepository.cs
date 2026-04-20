using Dapper;
using DedupeWaterfall.Core.Interfaces;
using DedupeWaterfall.Data.Infrastructure;

namespace DedupeWaterfall.Data.Repositories;

public class DedupeHitRepository : IDedupeHitRepository
{
    private readonly SqlConnectionFactory _connectionFactory;

    public DedupeHitRepository(SqlConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task<long> InsertHitAsync(
        long runId, long leadId, int lenderId,
        int sequenceOrder, CancellationToken ct)
    {
        const string sql = """
            INSERT INTO dedupe_hit (run_id, lead_id, lender_id, sequence_order, status, created_at)
            OUTPUT INSERTED.hit_id
            VALUES (@RunId, @LeadId, @LenderId, @SequenceOrder, 'Pending', GETUTCDATE())
            """;

        using var connection = _connectionFactory.CreateConnection();
        return await connection.ExecuteScalarAsync<long>(
            new CommandDefinition(sql,
                new
                {
                    RunId = runId,
                    LeadId = leadId,
                    LenderId = lenderId,
                    SequenceOrder = sequenceOrder
                },
                cancellationToken: ct));
    }
}
