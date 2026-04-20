using Confluent.Kafka;
using DedupeWaterfall.Core.Interfaces;
using DedupeWaterfall.Data.Infrastructure;
using DedupeWaterfall.Data.Options;
using DedupeWaterfall.Data.Repositories;
using DedupeWaterfall.Orchestrator.Infrastructure;
using DedupeWaterfall.Orchestrator.Options;
using DedupeWaterfall.Orchestrator.Services;
using DedupeWaterfall.Orchestrator.Workers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

// ----- Options -----
builder.Services.Configure<SqlOptions>(builder.Configuration.GetSection(SqlOptions.SectionName));
builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.SectionName));
builder.Services.Configure<OrchestratorOptions>(builder.Configuration.GetSection(OrchestratorOptions.SectionName));

// ----- Infrastructure -----
builder.Services.AddSingleton<SqlConnectionFactory>();
builder.Services.AddSingleton<KafkaConsumerFactory>();

// Kafka producer — shared singleton used by OrchestratorService
builder.Services.AddSingleton<IProducer<string, string>>(sp =>
{
    var kafkaOptions = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<KafkaOptions>>().Value;
    var config = new ProducerConfig
    {
        BootstrapServers = kafkaOptions.BootstrapServers,
        Acks = Acks.All,
        EnableIdempotence = true,
    };
    return new ProducerBuilder<string, string>(config).Build();
});

// ----- Repositories -----
builder.Services.AddScoped<IWaterfallSnapshotRepository, WaterfallSnapshotRepository>();
builder.Services.AddScoped<ILeadRunStateRepository, LeadRunStateRepository>();

// ----- Services -----
builder.Services.AddScoped<IOrchestratorService, OrchestratorService>();

// ----- Workers -----
builder.Services.AddHostedService<LeadQueuedWorker>();
builder.Services.AddHostedService<LenderResultWorker>();

var host = builder.Build();
host.Run();

