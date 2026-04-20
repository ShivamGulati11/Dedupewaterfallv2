using System.Collections.Concurrent;
using System.Text.Json;
using Confluent.Kafka;
using DedupeWaterfall.Core.Interfaces;
using DedupeWaterfall.Core.Kafka;
using DedupeWaterfall.Core.Models;
using DedupeWaterfall.Orchestrator.Infrastructure;
using DedupeWaterfall.Orchestrator.Options;
using DedupeWaterfall.Orchestrator.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DedupeWaterfall.Orchestrator.Workers;

public class LenderResultWorker : BackgroundService
{
    private readonly KafkaConsumerFactory        _consumerFactory;
    private readonly IServiceScopeFactory        _scopeFactory;
    private readonly IKafkaProducer              _kafkaProducer;
    private readonly OrchestratorOptions         _orchestratorOptions;
    private readonly string                      _groupId;
    private readonly ILogger<LenderResultWorker> _logger;

    private readonly ConcurrentDictionary<Guid, int> _failureCounts = new();

    public LenderResultWorker(
        KafkaConsumerFactory       consumerFactory,
        IServiceScopeFactory       scopeFactory,
        IKafkaProducer             kafkaProducer,
        IOptions<KafkaOptions>     kafkaOptions,
        IOptions<OrchestratorOptions> orchestratorOptions,
        ILogger<LenderResultWorker> logger)
    {
        _consumerFactory     = consumerFactory;
        _scopeFactory        = scopeFactory;
        _kafkaProducer       = kafkaProducer;
        _orchestratorOptions = orchestratorOptions.Value;
        _groupId             = kafkaOptions.Value.ConsumerGroups.LenderResult;
        _logger              = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var consumer = _consumerFactory.CreateConsumer(_groupId);
        consumer.Subscribe(KafkaTopics.LenderResult);

        _logger.LogInformation(
            "[LenderResultWorker] Subscribed to {Topic} with group {Group}",
            KafkaTopics.LenderResult, _groupId);

        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<string, string>? result  = null;
            LenderResultMessage?          message = null;

            try
            {
                result = consumer.Consume(stoppingToken);

                if (result?.Message?.Value is null)
                    continue;

                message = JsonSerializer.Deserialize<LenderResultMessage>(
                    result.Message.Value)
                    ?? throw new InvalidOperationException(
                        "Deserialized message is null.");

                await using var scope = _scopeFactory.CreateAsyncScope();
                var service = scope.ServiceProvider
                    .GetRequiredService<OrchestratorService>();

                await service.ProcessLenderResultAsync(message, stoppingToken);

                consumer.StoreOffset(result);
                consumer.Commit(result);

                _failureCounts.TryRemove(message.MessageId, out _);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                var messageId = message?.MessageId ?? Guid.Empty;
                var leadId    = message?.LeadId    ?? 0L;
                var runId     = message?.RunId     ?? 0L;

                _logger.LogError(ex,
                    "[LenderResultWorker] Error processing MessageId={MessageId} " +
                    "LeadId={LeadId} RunId={RunId}",
                    messageId, leadId, runId);

                _failureCounts.TryGetValue(messageId, out int failures);
                failures++;
                _failureCounts[messageId] = failures;

                if (failures >= _orchestratorOptions.MaxRetryCount && result is not null)
                {
                    _logger.LogWarning(
                        "[LenderResultWorker] MessageId={MessageId} exceeded " +
                        "MaxRetryCount={MaxRetry}. Routing to DLQ.",
                        messageId, _orchestratorOptions.MaxRetryCount);

                    await RouteToDlqAsync(result, stoppingToken);
                    consumer.StoreOffset(result);
                    consumer.Commit(result);
                    _failureCounts.TryRemove(messageId, out _);
                }
                else if (_orchestratorOptions.RetryDelayMs > 0)
                {
                    await Task.Delay(_orchestratorOptions.RetryDelayMs, stoppingToken);
                }
            }
        }

        consumer.Close();
    }

    private async Task RouteToDlqAsync(
        ConsumeResult<string, string> result, CancellationToken ct)
    {
        // Re-publish the raw value to the DLQ topic using the singleton producer
        await _kafkaProducer.ProduceAsync(
            topic:   $"{KafkaTopics.LenderResult}.dlq",
            key:     result.Message.Key ?? string.Empty,
            message: result.Message.Value,
            ct:      ct);
    }
}
