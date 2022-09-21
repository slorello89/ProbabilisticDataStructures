using StackExchange.Redis;

namespace ProbabilisticDataStructures;

public class RedisBruteForce : IMethod
{
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
            tasks.Add(Db.SortedSetIncrementAsync(_sortedSetName, word,1));
        }

        await Task.WhenAll(tasks);
    }

    public async Task<bool> PresenceCheck(string word)
    {
        return await Db.SortedSetScoreAsync(_sortedSetName, word) != null;
    }

    public async Task<long> ItemCount(string word)
    {
        var res = (long?)await Db.SortedSetScoreAsync(_sortedSetName, word);

        return res ?? 0;
    }

    public async Task<long> CardinalityCheck()
    {
        return await Db.SortedSetLengthAsync(_sortedSetName);
    }

    public async Task<string[]> TopKCheck(long top)
    {
        return (await Db.SortedSetRangeByRankAsync(_sortedSetName, 0, top, Order.Descending)).Select(s=>(string)s).ToArray();
    }

    public async Task GetSize(IDictionary<string, long> dictionary)
    {
        dictionary["bruteForce"] = (long)await Db.ExecuteAsync("MEMORY", "USAGE", _sortedSetName);
    }
}