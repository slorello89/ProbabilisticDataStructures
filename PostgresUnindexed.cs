using System.Data.Common;
using Npgsql;

namespace ProbabilisticDataStructures;

public class PostgresUnindexed : IMethod
{
    public string Name => "Postgres Unindexed";
    private const string TableName = "wordsunindexed";
    private readonly NpgsqlConnection _conn;

    public PostgresUnindexed(NpgsqlConnection conn)
    {
        _conn = conn;
    }

    public async Task Initialize(IEnumerable<string> words)
    {
        await using (var cmd = new NpgsqlCommand(
                         $"CREATE TABLE IF NOT EXISTS {TableName} (id serial PRIMARY KEY, word VARCHAR(50) NOT NULL)", _conn))
        {
            await cmd.ExecuteNonQueryAsync();
        }

        var i = 0;
        using (var writer =  await _conn.BeginBinaryImportAsync($"copy public.{TableName} from STDIN (FORMAT BINARY)"))
        {
            foreach (var word in words)
            {
                await writer.StartRowAsync();
                await writer.WriteAsync(i++);
                await writer.WriteAsync(word);
            }
        
            await writer.CompleteAsync();
        }
    }

    public async Task<bool> PresenceCheck(string word)
    {
        await using (var cmd = new NpgsqlCommand($"SELECT count(*) FROM {TableName} WHERE word = '{word}'", _conn))
        {
            var res = await cmd.ExecuteScalarAsync();
            return (long) res! > 0;

        }
    }

    public async Task<long> ItemCount(string word)
    {
        await using (var cmd = new NpgsqlCommand($"SELECT count(*) FROM {TableName} WHERE word = '{word}'", _conn))
        {
            var res = await cmd.ExecuteScalarAsync();
            return (long) res!;

        }
    }

    public async Task<long> CardinalityCheck()
    {
        await using (var cmd = new NpgsqlCommand($"SELECT count(DISTINCT word) FROM {TableName}", _conn))
        {
            var res = await cmd.ExecuteScalarAsync();
            return (long) res!;

        }
    }

    public async Task<string[]> TopKCheck(long top)
    {
        var words = new List<string>();
        await using (var cmd = new NpgsqlCommand(
                         $"SELECT word FROM {TableName} GROUP BY word ORDER BY count(word) DESC LIMIT 10", _conn))
        {
            await using (var res = await cmd.ExecuteReaderAsync())
            {
                foreach (var result in res)
                {
                    var reader = (DbDataRecord) result;
                    words.Add(reader.GetString(0));
                }
            }
        }

        return words.ToArray();
    }

    public async Task GetSize(IDictionary<string, long> dictionary)
    {
        await using (var cmd = new NpgsqlCommand($"SELECT pg_indexes_size('{TableName}') + pg_table_size('{TableName}');", _conn))
        {
            dictionary["unindexedSize"] = (long) (await cmd.ExecuteScalarAsync())!;
        }
    }
}