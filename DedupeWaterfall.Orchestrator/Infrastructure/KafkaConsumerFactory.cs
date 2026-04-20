using Confluent.Kafka;
using DedupeWaterfall.Orchestrator.Options;
using Microsoft.Extensions.Options;

namespace DedupeWaterfall.Orchestrator.Infrastructure;

/// <summary>
/// Builds Kafka consumers with a standardised configuration derived from <see cref="KafkaOptions"/>.
/// </summary>
public sealed class KafkaConsumerFactory
{
    private readonly KafkaOptions _options;

    public KafkaConsumerFactory(IOptions<KafkaOptions> options)
    {
        _options = options.Value;
    }

    public IConsumer<string, string> Create(string groupIdSuffix = "")
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = string.IsNullOrWhiteSpace(groupIdSuffix)
                ? _options.ConsumerGroupId
                : $"{_options.ConsumerGroupId}-{groupIdSuffix}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            SecurityProtocol = ParseSecurityProtocol(_options.SecurityProtocol),
        };

        if (!string.IsNullOrWhiteSpace(_options.SaslMechanism))
        {
            config.SaslMechanism = Enum.Parse<SaslMechanism>(_options.SaslMechanism, ignoreCase: true);
            config.SaslUsername = _options.SaslUsername;
            config.SaslPassword = _options.SaslPassword;
        }

        return new ConsumerBuilder<string, string>(config).Build();
    }

    private static SecurityProtocol ParseSecurityProtocol(string value) =>
        Enum.TryParse<SecurityProtocol>(value, ignoreCase: true, out var result)
            ? result
            : SecurityProtocol.Plaintext;
}
