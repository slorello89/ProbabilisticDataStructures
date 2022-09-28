// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using Npgsql;
using ProbabilisticDataStructures;
using StackExchange.Redis;

// connect to postgres with contrived connection string
var connString = "Host=localhost;Username=postgres;Password=secretpassword;Database=postgres";
await using var conn = new NpgsqlConnection(connString);
await conn.OpenAsync();

// connect to redis with contrived connection string
var muxer = ConnectionMultiplexer.Connect("localhost,asyncTimeout=25000,syncTimeout=15000");
var db = muxer.GetDatabase();

// flush out redis
await db.ExecuteAsync("FLUSHDB");

// flush out postgres
await using (var dropCommand = new NpgsqlCommand("DROP SCHEMA public CASCADE; CREATE SCHEMA public", conn))
{
    await dropCommand.ExecuteNonQueryAsync();
}

// likely incomplete list of delimiting characters within the book
char[] delimiterChars = { ' ', ',', '.', ':', '\t', '\n', '—', '?', '"', ';', '!', '’', '\r', '\'', '(', ')', '”' };

// pull in text of Moby Dick
var text = await File.ReadAllTextAsync("data/moby-dick.txt");

// split words out from text
var words = text.Split(delimiterChars).Where(s=>!string.IsNullOrWhiteSpace(s)).Select(s=>s.ToLower()).ToArray();

// initialize our different methods
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
    var res = await method.PresenceCheck(word) ? "was" : "was not";
    Console.WriteLine($"Method: {method.Name} determined that '{word}' {res} present");
    watch.Stop();
    return watch.ElapsedMilliseconds;
}

async Task<long> MeasureQueryCount(IMethod method, string word)
{
    var watch = Stopwatch.StartNew();
    var res = await method.ItemCount(word);
    Console.WriteLine($"Method: {method.Name} counted {res} occurrences of '{word}'");
    watch.Stop();
    return watch.ElapsedMilliseconds;
}

async Task<long> MeasureCardinality(IMethod method)
{
    var watch = Stopwatch.StartNew();
    var res = await method.CardinalityCheck();
    Console.WriteLine($"Method: {method.Name} counted a total cardinality of {res}");
    watch.Stop();
    return watch.ElapsedMilliseconds;
}

async Task<long> MeasureTop(IMethod method)
{
    var watch = Stopwatch.StartNew();
    var res = string.Join(",", await method.TopKCheck(10));
    Console.WriteLine($"Method: {method.Name} reported top words: {res}");
    watch.Stop();
    return watch.ElapsedMilliseconds;
}

long postgresUnindexedTime;
long postgresIndexedTime;
long redisBruteForceTime;
long redisProbabilisticTime;

// Initialize all of our data stores
Console.WriteLine("==========Init==========");
postgresUnindexedTime = await MeasureMethodInit(postgresUnindexed, words);
postgresIndexedTime = await MeasureMethodInit(postgresIndexed, words);
redisBruteForceTime = await MeasureMethodInit(redisBruteForce, words);
redisProbabilisticTime = await MeasureMethodInit(redisProbabilistic, words);

Console.WriteLine($"pg unindexed:        \t{postgresUnindexedTime}");
Console.WriteLine($"pg indexed:          \t{postgresIndexedTime}");
Console.WriteLine($"redis brute force:   \t{redisBruteForceTime}");
Console.WriteLine($"redis probabilistic: \t{redisProbabilisticTime}");

// check Presence in each of the data stores
Console.WriteLine("==========Presence Check==========");
Console.WriteLine("==========Results===========");
var word = "the";
postgresUnindexedTime = await MeasureQueryPresence(postgresUnindexed, word);
postgresIndexedTime = await MeasureQueryPresence(postgresIndexed, word);
redisBruteForceTime = await MeasureQueryPresence(redisBruteForce, word);
redisProbabilisticTime = await MeasureQueryPresence(redisProbabilistic, word);

Console.WriteLine("=========Times=============");
Console.WriteLine($"pg unindexed:        \t{postgresUnindexedTime}");
Console.WriteLine($"pg indexed:          \t{postgresIndexedTime}");
Console.WriteLine($"redis brute force:   \t{redisBruteForceTime}");
Console.WriteLine($"redis probabilistic: \t{redisProbabilisticTime}");

// Check word occurrences in each of the data stores
Console.WriteLine("==========Item Count==========");
Console.WriteLine("==========Results=========");
postgresUnindexedTime = await MeasureQueryCount(postgresUnindexed, word);
postgresIndexedTime = await MeasureQueryCount(postgresIndexed, word);
redisBruteForceTime = await MeasureQueryCount(redisBruteForce, word);
redisProbabilisticTime = await MeasureQueryCount(redisProbabilistic, word);

Console.WriteLine("==========Times==========");
Console.WriteLine($"pg unindexed:        \t{postgresUnindexedTime}");
Console.WriteLine($"pg indexed:          \t{postgresIndexedTime}");
Console.WriteLine($"redis brute force:   \t{redisBruteForceTime}");
Console.WriteLine($"redis probabilistic: \t{redisProbabilisticTime}");

// check cardinality of each of the data stores
Console.WriteLine("==========Cardinality==========");
Console.WriteLine("==========Results=========");

postgresUnindexedTime = await MeasureCardinality(postgresUnindexed);
postgresIndexedTime = await MeasureCardinality(postgresIndexed);
redisBruteForceTime = await MeasureCardinality(redisBruteForce);
redisProbabilisticTime = await MeasureCardinality(redisProbabilistic);

Console.WriteLine("==========Times==========");
Console.WriteLine($"pg unindexed:        \t{postgresUnindexedTime}");
Console.WriteLine($"pg indexed:          \t{postgresIndexedTime}");
Console.WriteLine($"redis brute force:   \t{redisBruteForceTime}");
Console.WriteLine($"redis probabilistic: \t{redisProbabilisticTime}");

// determine topK in each data store
Console.WriteLine("==========Top K==========");
Console.WriteLine("==========Results=========");

postgresUnindexedTime = await MeasureTop(postgresUnindexed);
postgresIndexedTime = await MeasureTop(postgresIndexed);
redisBruteForceTime = await MeasureTop(redisBruteForce);
redisProbabilisticTime = await MeasureTop(redisProbabilistic);

Console.WriteLine("==========Times==========");
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