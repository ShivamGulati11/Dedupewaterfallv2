using Confluent.Kafka;
using DedupeWaterfall.Core.Contracts;
using DedupeWaterfall.Core.Interfaces;
using DedupeWaterfall.Orchestrator.Infrastructure;
using DedupeWaterfall.Orchestrator.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace DedupeWaterfall.Orchestrator.Workers;

/// <summary>
/// Background worker that consumes messages from <c>dedupe.leads.queued</c>
/// and delegates to <see cref="IOrchestratorService"/> to trigger the first
/// eligible lender in the waterfall.
/// </summary>
public sealed class LeadQueuedWorker : BackgroundService
{
    private readonly IOrchestratorService _orchestrator;
    private readonly KafkaConsumerFactory _consumerFactory;
    private readonly KafkaOptions _kafkaOptions;
    private readonly OrchestratorOptions _orchestratorOptions;
    private readonly ILogger<LeadQueuedWorker> _logger;

    public LeadQueuedWorker(
        IOrchestratorService orchestrator,
        KafkaConsumerFactory consumerFactory,
        IOptions<KafkaOptions> kafkaOptions,
        IOptions<OrchestratorOptions> orchestratorOptions,
        ILogger<LeadQueuedWorker> logger)
    {
        _orchestrator = orchestrator;
        _consumerFactory = consumerFactory;
        _kafkaOptions = kafkaOptions.Value;
        _orchestratorOptions = orchestratorOptions.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "{Worker} starting — subscribing to '{Topic}'.",
            nameof(LeadQueuedWorker), _kafkaOptions.LeadsQueuedTopic);

        using var consumer = _consumerFactory.Create("leads");
        consumer.Subscribe(_kafkaOptions.LeadsQueuedTopic);

        var timeout = TimeSpan.FromMilliseconds(_orchestratorOptions.ConsumePollTimeoutMs);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? result;
                try
                {
                    result = consumer.Consume(timeout);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Kafka consume error on topic '{Topic}'.", _kafkaOptions.LeadsQueuedTopic);
                    continue;
                }

                if (result is null)
                    continue;

                LeadQueuedMessage? message;
                try
                {
                    message = JsonSerializer.Deserialize<LeadQueuedMessage>(result.Message.Value);
                }
                catch (JsonException ex)
                {
                    _logger.LogError(ex, "Failed to deserialise LeadQueuedMessage. Offset={Offset}.", result.Offset);
                    consumer.StoreOffset(result);
                    consumer.Commit(result);
                    continue;
                }

                if (message is null)
                {
                    _logger.LogWarning("Received null LeadQueuedMessage. Offset={Offset}.", result.Offset);
                    consumer.StoreOffset(result);
                    consumer.Commit(result);
                    continue;
                }

                try
                {
                    await _orchestrator.HandleLeadQueuedAsync(message, stoppingToken);
                    consumer.StoreOffset(result);
                    consumer.Commit(result);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Unhandled error processing LeadQueuedMessage. RunId={RunId}.", message.RunId);
                }
            }
        }
        finally
        {
            consumer.Close();
            _logger.LogInformation("{Worker} stopped.", nameof(LeadQueuedWorker));
        }
    }
}
