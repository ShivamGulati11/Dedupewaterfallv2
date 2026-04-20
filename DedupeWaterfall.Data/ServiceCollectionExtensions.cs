using DedupeWaterfall.Core.Interfaces;
using DedupeWaterfall.Data.Infrastructure;
using DedupeWaterfall.Data.Repositories;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace DedupeWaterfall.Data;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers all Data-layer services (repositories, connection factory).
    /// Called from Program.cs via <c>.AddDedupeData(builder.Configuration)</c>.
    /// </summary>
    public static IServiceCollection AddDedupeData(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.AddSingleton<SqlConnectionFactory>();

        services.AddScoped<IWaterfallConfigRepository, WaterfallConfigRepository>();
        services.AddScoped<IWaterfallRunRepository, WaterfallRunRepository>();
        services.AddScoped<IDedupeHitRepository, DedupeHitRepository>();
        services.AddScoped<IEventLogRepository, EventLogRepository>();

        return services;
    }
}
