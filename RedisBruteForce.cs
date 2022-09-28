using StackExchange.Redis;

namespace ProbabilisticDataStructures;

public class RedisBruteForce : IMethod
{
    public string Name => "Redis Brute Force";
    private IDatabase Db => _muxer.GetDatabase();
    private ConnectionMultiplexer _muxer;
    private static readonly string _sortedSetName = "sortedSet";


    public RedisBruteForce(ConnectionMultiplexer muxer)
    {
        _muxer = muxer;
    }

    public async Task Initialize(IEnumerable<string> words)
    {
        var tasks = new List<Task>();
        foreach (var word in words)
        {
            // Increments the sorted set for the word for each occurence in the book.
            tasks.Add(Db.SortedSetIncrementAsync(_sortedSetName, word,1));
        }

        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Checks whether the word has been added yet.
    /// </summary>
    /// <param name="word"></param>
    /// <returns></returns>
    /// <remarks>https://redis.io/commands/zscore/</remarks>
    public async Task<bool> PresenceCheck(string word)
    {
        return await Db.SortedSetScoreAsync(_sortedSetName, word) != null;
    }

    /// <summary>
    /// Checks how many time the word hs occured in the sorted set
    /// </summary>
    /// <param name="word"></param>
    /// <returns></returns>
    /// <remarks>https://redis.io/commands/zscore/</remarks>
    public async Task<long> ItemCount(string word)
    {
        var res = (long?)await Db.SortedSetScoreAsync(_sortedSetName, word);

        return res ?? 0;
    }

    /// <summary>
    /// Checks the cardinality of the sortedSet
    /// </summary>
    /// <returns></returns>
    /// <remarks>https://redis.io/commands/zcard/</remarks>
    public async Task<long> CardinalityCheck()
    {
        return await Db.SortedSetLengthAsync(_sortedSetName);
    }

    /// <summary>
    /// Checks The top word occurrences
    /// </summary>
    /// <param name="top"></param>
    /// <returns></returns>
    /// <remarks>https://redis.io/commands/zrange</remarks>
    public async Task<string[]> TopKCheck(long top)
    {
        return (await Db.SortedSetRangeByRankAsync(_sortedSetName, 0, top-1, Order.Descending)).Select(s=>(string)s!).ToArray();
    }

    /// <summary>
    /// Gets the size of the memory usage of the sorted set
    /// </summary>
    /// <param name="dictionary"></param>
    /// <remarks>https://redis.io/commands/memory-usage/</remarks>
    public async Task GetSize(IDictionary<string, long> dictionary)
    {
        dictionary["bruteForce"] = (long)await Db.ExecuteAsync("MEMORY", "USAGE", _sortedSetName);
    }
}