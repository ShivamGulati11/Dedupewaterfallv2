namespace DedupeWaterfall.Data.Options;

public class SqlOptions
{
    public const string SectionName = "Sql";

    public string ConnectionString { get; set; } = string.Empty;
}
