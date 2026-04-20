using DedupeWaterfall.Core.Interfaces;
using DedupeWaterfall.Data;
using DedupeWaterfall.Orchestrator.Infrastructure;
using DedupeWaterfall.Orchestrator.Options;
using DedupeWaterfall.Orchestrator.Services;
using DedupeWaterfall.Orchestrator.Workers;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"))
    .Configure<OrchestratorOptions>(builder.Configuration.GetSection("Orchestrator"))
    .AddDedupeData(builder.Configuration)
    .AddSingleton<KafkaConsumerFactory>()
    .AddSingleton<IKafkaProducer, KafkaProducerService>()
    .AddScoped<OrchestratorService>()
    .AddHostedService<LeadQueuedWorker>()
    .AddHostedService<LenderResultWorker>();

builder.Logging.AddConsole();

var host = builder.Build();
await host.RunAsync();
