using System.Text.Json;
using DedupeWaterfall.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace DedupeWaterfall.Data.Repositories;

/// <summary>
/// Buffers waterfall events into an in-memory queue.
/// In production, swap the in-memory buffer for Redis Streams
/// (e.g. StackExchange.Redis XADD) without changing the interface contract.
/// </summary>
public class EventLogRepository : IEventLogRepository
{
    private readonly ILogger<EventLogRepository> _logger;

    public EventLogRepository(ILogger<EventLogRepository> logger)
    {
        _logger = logger;
    }

    public Task BufferEventAsync(
        long runId, long leadId, int? lenderId,
        string eventType, object? payload, CancellationToken ct)
    {
        // Buffer to Redis Streams in production.
        // For now, log the event so that the pipeline is fully traceable.
        var payloadJson = payload is null ? "{}" : JsonSerializer.Serialize(payload);
        _logger.LogInformation(
            "[EventBuffer] RunId={RunId} LeadId={LeadId} LenderId={LenderId} " +
            "EventType={EventType} Payload={Payload}",
            runId, leadId, lenderId, eventType, payloadJson);

        return Task.CompletedTask;
    }
}
