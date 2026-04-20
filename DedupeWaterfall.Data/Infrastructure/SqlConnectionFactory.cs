using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System.Data;

namespace DedupeWaterfall.Data.Infrastructure;

public class SqlConnectionFactory
{
    private readonly string _connectionString;

    public SqlConnectionFactory(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("AzureSql")
            ?? throw new InvalidOperationException(
                "Connection string 'AzureSql' is not configured.");
    }

    public IDbConnection CreateConnection() =>
        new SqlConnection(_connectionString);
}
