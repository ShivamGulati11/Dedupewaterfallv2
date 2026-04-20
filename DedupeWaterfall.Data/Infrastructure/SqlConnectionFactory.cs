using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using DedupeWaterfall.Data.Options;
using System.Data;

namespace DedupeWaterfall.Data.Infrastructure;

/// <summary>
/// Opens new <see cref="SqlConnection"/> instances using the configured connection string.
/// </summary>
public sealed class SqlConnectionFactory
{
    private readonly string _connectionString;

    public SqlConnectionFactory(IOptions<SqlOptions> options)
    {
        _connectionString = options.Value.ConnectionString
            ?? throw new ArgumentException("SQL connection string must not be empty.", nameof(options));
    }

    public IDbConnection Create() => new SqlConnection(_connectionString);
}
