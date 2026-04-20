using Confluent.Kafka;
using DedupeWaterfall.Orchestrator.Options;
using Microsoft.Extensions.Options;

namespace DedupeWaterfall.Orchestrator.Infrastructure;

public class KafkaConsumerFactory
{
    private readonly KafkaOptions _options;

    public KafkaConsumerFactory(IOptions<KafkaOptions> options)
    {
        _options = options.Value;
    }

    public IConsumer<string, string> CreateConsumer(string groupId)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers      = _options.BootstrapServers,
            GroupId               = groupId,
            AutoOffsetReset       = AutoOffsetReset.Earliest,
            EnableAutoCommit      = false,
            EnableAutoOffsetStore = false,
            SessionTimeoutMs      = 45_000,
            MaxPollIntervalMs     = 300_000
        };

        return new ConsumerBuilder<string, string>(config).Build();
    }
}
