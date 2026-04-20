using System.Text.Json;
using Confluent.Kafka;
using DedupeWaterfall.Core.Interfaces;
using DedupeWaterfall.Orchestrator.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DedupeWaterfall.Orchestrator.Infrastructure;

public class KafkaProducerService : IKafkaProducer, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaProducerService> _logger;

    public KafkaProducerService(
        IOptions<KafkaOptions> options,
        ILogger<KafkaProducerService> logger)
    {
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            ClientId         = options.Value.ClientId,
            Acks             = Acks.All,
            EnableIdempotence = true
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task ProduceAsync<T>(
        string topic, string key, T message, CancellationToken ct)
    {
        var payload = JsonSerializer.Serialize(message);

        var kafkaMessage = new Message<string, string>
        {
            Key   = key,
            Value = payload
        };

        var result = await _producer.ProduceAsync(topic, kafkaMessage, ct);

        _logger.LogDebug(
            "Produced message to {Topic} partition {Partition} offset {Offset}",
            result.Topic, result.Partition, result.Offset);
    }

    public void Dispose() => _producer.Dispose();
}
