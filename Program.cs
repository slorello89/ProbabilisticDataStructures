// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using Npgsql;
using ProbabilisticDataStructures;
using StackExchange.Redis;


Console.WriteLine("Hello, World!");

var connString = "Host=localhost;Username=postgres;Password=secretpassword;Database=postgres";
await using var conn = new NpgsqlConnection(connString);
await conn.OpenAsync();
var muxer = ConnectionMultiplexer.Connect("localhost,asyncTimeout=25000,syncTimeout=15000");
var db = muxer.GetDatabase();
await db.ExecuteAsync("FLUSHDB");
await using (var dropCommand = new NpgsqlCommand("DROP SCHEMA public CASCADE; CREATE SCHEMA public", conn))
{
    await dropCommand.ExecuteNonQueryAsync();
}
char[] delimiterChars = { ' ', ',', '.', ':', '\t', '\n', '—', '?', '"', ';', '!', '’', '\r', '\'', '(', ')' };

var text = await File.ReadAllTextAsync("data/moby-dick.txt");
var words = text.Split(delimiterChars).Where(s=>!string.IsNullOrWhiteSpace(s)).Select(s=>s.ToLower()).ToArray();

var postgresUnindexed = new PostgresUnindexed(conn);
var postgresIndexed = new PostgresIndexed(conn);
var redisBruteForce = new RedisBruteForce(muxer);
var redisProbabilistic = new RedisProbabilistic(muxer);

async Task<long> MeasureMethodInit(IMethod method, IEnumerable<string> words)
{
    var watch = Stopwatch.StartNew();
    await method.Initialize(words);
    watch.Stop();
    return watch.ElapsedMilliseconds;
}

async Task<long> MeasureQueryPresence(IMethod method, string word)
{
    var watch = Stopwatch.StartNew();
    await method.PresenceCheck(word);
    watch.Stop();
    return watch.ElapsedMilliseconds;
}

async Task<long> MeasureQueryCount(IMethod method, string word)
{
    var watch = Stopwatch.StartNew();
    await method.ItemCount(word);
    watch.Stop();
    return watch.ElapsedMilliseconds;
}

async Task<long> MeasureCardinality(IMethod method)
{
    var watch = Stopwatch.StartNew();
    await method.CardinalityCheck();
    watch.Stop();
    return watch.ElapsedMilliseconds;
}

async Task<long> MeasureTop(IMethod method)
{
    var watch = Stopwatch.StartNew();
    await method.TopKCheck(10);
    watch.Stop();
    return watch.ElapsedMilliseconds;
}

long postgresUnindexedTime;
long postgresIndexedTime;
long redisBruteForceTime;
long redisProbabilisticTime;

Console.WriteLine("==========Init==========");
postgresUnindexedTime = await MeasureMethodInit(postgresUnindexed, words);
postgresIndexedTime = await MeasureMethodInit(postgresIndexed, words);
redisBruteForceTime = await MeasureMethodInit(redisBruteForce, words);
redisProbabilisticTime = await MeasureMethodInit(redisProbabilistic, words);

Console.WriteLine($"pg unindexed:        \t{postgresUnindexedTime}");
Console.WriteLine($"pg indexed:          \t{postgresIndexedTime}");
Console.WriteLine($"redis brute force:   \t{redisBruteForceTime}");
Console.WriteLine($"redis probabilistic: \t{redisProbabilisticTime}");

Console.WriteLine("==========Presence Check==========");

var word = "the";
postgresUnindexedTime = await MeasureQueryPresence(postgresUnindexed, word);
postgresIndexedTime = await MeasureQueryPresence(postgresIndexed, word);
redisBruteForceTime = await MeasureQueryPresence(redisBruteForce, word);
redisProbabilisticTime = await MeasureQueryPresence(redisProbabilistic, word);

Console.WriteLine($"pg unindexed:        \t{postgresUnindexedTime}");
Console.WriteLine($"pg indexed:          \t{postgresIndexedTime}");
Console.WriteLine($"redis brute force:   \t{redisBruteForceTime}");
Console.WriteLine($"redis probabilistic: \t{redisProbabilisticTime}");

Console.WriteLine("==========Item Count==========");

postgresUnindexedTime = await MeasureQueryCount(postgresUnindexed, word);
postgresIndexedTime = await MeasureQueryCount(postgresIndexed, word);
redisBruteForceTime = await MeasureQueryCount(redisBruteForce, word);
redisProbabilisticTime = await MeasureQueryCount(redisProbabilistic, word);

Console.WriteLine($"pg unindexed:        \t{postgresUnindexedTime}");
Console.WriteLine($"pg indexed:          \t{postgresIndexedTime}");
Console.WriteLine($"redis brute force:   \t{redisBruteForceTime}");
Console.WriteLine($"redis probabilistic: \t{redisProbabilisticTime}");

Console.WriteLine("==========Cardinality==========");

postgresUnindexedTime = await MeasureCardinality(postgresUnindexed);
postgresIndexedTime = await MeasureCardinality(postgresIndexed);
redisBruteForceTime = await MeasureCardinality(redisBruteForce);
redisProbabilisticTime = await MeasureCardinality(redisProbabilistic);

Console.WriteLine($"pg unindexed:        \t{postgresUnindexedTime}");
Console.WriteLine($"pg indexed:          \t{postgresIndexedTime}");
Console.WriteLine($"redis brute force:   \t{redisBruteForceTime}");
Console.WriteLine($"redis probabilistic: \t{redisProbabilisticTime}");

Console.WriteLine("==========Top K==========");

postgresUnindexedTime = await MeasureTop(postgresUnindexed);
postgresIndexedTime = await MeasureTop(postgresIndexed);
redisBruteForceTime = await MeasureTop(redisBruteForce);
redisProbabilisticTime = await MeasureTop(redisProbabilistic);

Console.WriteLine($"pg unindexed:        \t{postgresUnindexedTime}");
Console.WriteLine($"pg indexed:          \t{postgresIndexedTime}");
Console.WriteLine($"redis brute force:   \t{redisBruteForceTime}");
Console.WriteLine($"redis probabilistic: \t{redisProbabilisticTime}");

Console.WriteLine("==========Sizes==========");
var sizeDictionary = new Dictionary<string, long>();
await postgresUnindexed.GetSize(sizeDictionary);
await postgresIndexed.GetSize(sizeDictionary);
await redisBruteForce.GetSize(sizeDictionary);
await redisProbabilistic.GetSize(sizeDictionary);
foreach (var kvp in sizeDictionary)
{
    Console.WriteLine($"{kvp.Key}: \t\t{kvp.Value}");
}