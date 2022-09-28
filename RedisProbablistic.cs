using StackExchange.Redis;

namespace ProbabilisticDataStructures;

public class RedisProbabilistic : IMethod
{
    public string Name => "Redis Probabilistic";
    private IDatabase Db => _muxer.GetDatabase();
    private readonly string _bfName = "bloom";
    private readonly string _cmsName = "cms";
    private readonly string _topKName = "topK";
    private readonly string _hllName = "hll";
    private readonly ConnectionMultiplexer _muxer;

    public RedisProbabilistic(ConnectionMultiplexer muxer)
    {
        _muxer = muxer;
    }

    public async Task Initialize(IEnumerable<string> words)
    {
        var arr = words.Select(x=>(RedisValue)x).ToArray();
        var cmsList = arr.Aggregate(new List<object>(){_cmsName}, (list, s) =>
        {
            list.Add(s);
            list.Add(1);
            return list;
        }).ToArray();

        var bloomList = arr.Aggregate(new List<object> {_bfName}, (list, value) =>
        {
            list.Add(value);
            return list;
        }).ToArray();

        var topKList = arr.Aggregate(new List<object> {_topKName}, (list, value) =>
        {
            list.Add(value);
            return list;
        }).ToArray();
        
        
        var tasks = new List<Task>();
        // Initializes the sketch with desired probabilities, alternatively you can Init by dimensions
        // see https://redis.io/commands/cms.initbyprob/ and https://redis.io/commands/cms.initbydim/
        tasks.Add(Db.ExecuteAsync("CMS.INITBYPROB", _cmsName,0.01,0.01));
        
        // Increments each word from the book for each occurrence by 1, leverage varadicity of command
        // to increase performance and lower number of commands we need to execute against redis
        // see: https://redis.io/commands/cms.incrby/
        tasks.Add(Db.ExecuteAsync("CMS.INCRBY", cmsList.ToArray()));
        
        // Reserves a bloom filter with the given error rate and capacity
        // see: https://redis.io/commands/bf.reserve/
        tasks.Add(Db.ExecuteAsync("BF.RESERVE", _bfName, 0.01, 40000));
        
        // Adds all the words from the book in one go
        // See https://redis.io/commands/bf.madd/ and https://redis.io/commands/bf.add/
        tasks.Add(Db.ExecuteAsync("BF.MADD", bloomList.ToArray()));
        
        // Reserves a topK for the given number of top instances with a given width and depth
        // see: https://redis.io/commands/topk.reserve/
        tasks.Add(Db.ExecuteAsync("TOPK.RESERVE", _topKName, 10, 20, 10, .925));
        
        // Adds all the words from the book to the top-k
        // see: https://redis.io/commands/topk.add/
        tasks.Add(Db.ExecuteAsync("TOPK.ADD", topKList.ToArray()));
        
        // Adds all the items to the HyperLogLog
        // See: https://redis.io/commands/pfadd/
        tasks.Add(Db.HyperLogLogAddAsync(_hllName, arr));
        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Uses <see href="https://redis.io/commands/bf.exists/">BF.EXISTS</see>
    /// to determine whether an item exists in the Bloom Filter or not.
    /// </summary>
    /// <param name="word">the word to check</param>
    /// <returns></returns>
    public async Task<bool> PresenceCheck(string word)
    {
        return(long)await Db.ExecuteAsync("BF.EXISTS", _bfName, word) == 1;
    }

    /// <summary>
    /// Gets the number of occurrences of the word from Redis
    /// </summary>
    /// <param name="word">The word to query</param>
    /// <returns>Number of occurrences</returns>
    /// <remarks>https://redis.io/commands/CMS.QUERY</remarks>
    public async Task<long> ItemCount(string word)
    {
        return (long) await Db.ExecuteAsync("CMS.QUERY", _cmsName, word);
    }

    /// <summary>
    /// Checks the cardinality of the set with HyperLogLogLength
    /// </summary>
    /// <returns>The cardinality</returns>
    /// <remarks>https://redis.io/commands/pfcount/</remarks>
    public async Task<long> CardinalityCheck()
    {
        return await Db.HyperLogLogLengthAsync(_hllName);
    }

    /// <summary>
    /// Gets the list of top items from the top-k
    /// </summary>
    /// <param name="top">The number of top items to pull</param>
    /// <returns></returns>
    /// <remarks>https://redis.io/commands/topk.list</remarks>
    public async Task<string[]> TopKCheck(long top)
    {
        return ((RedisResult[]) await Db.ExecuteAsync("TOPK.LIST", _topKName)).Select(s=>(string)s).ToArray();
    }

    /// <summary>
    /// Checks the sizes of the involved data structures.
    /// </summary>
    /// <param name="dictionary">The dictionary to output the memory sizes to</param>
    /// <remarks>https://redis.io/commands/memory-usage/</remarks>
    public async Task GetSize(IDictionary<string, long> dictionary)
    {        
        dictionary["bfSize"] = (long) await Db.ExecuteAsync("MEMORY", "USAGE", _bfName);
        dictionary["cmsSize"] = (long) await Db.ExecuteAsync("MEMORY", "USAGE", _cmsName);
        dictionary["hllSize"] = (long) await Db.ExecuteAsync("MEMORY", "USAGE", _hllName);
        dictionary["topKSize"] = (long) await Db.ExecuteAsync("MEMORY", "USAGE", _topKName);
    }
}