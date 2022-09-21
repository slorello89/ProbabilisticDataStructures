using StackExchange.Redis;

namespace ProbabilisticDataStructures;

public class RedisProbabilistic : IMethod
{
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
        tasks.Add(Db.ExecuteAsync("CMS.INITBYPROB", _cmsName,0.01,0.01));
        tasks.Add(Db.ExecuteAsync("CMS.INCRBY", cmsList.ToArray()));
        tasks.Add(Db.ExecuteAsync("BF.RESERVE", _bfName, 0.01, 40000));
        tasks.Add(Db.ExecuteAsync("BF.MADD", bloomList.ToArray()));
        tasks.Add(Db.ExecuteAsync("TOPK.RESERVE", _topKName, 10));
        tasks.Add(Db.ExecuteAsync("TOPK.ADD", topKList.ToArray()));
        tasks.Add(Db.HyperLogLogAddAsync(_hllName, arr));
        await Task.WhenAll(tasks);
    }

    public async Task<bool> PresenceCheck(string word)
    {
        return(long)await Db.ExecuteAsync("BF.EXISTS", _bfName, word) == 1;
    }

    public async Task<long> ItemCount(string word)
    {
        return (long) await Db.ExecuteAsync("CMS.QUERY", _cmsName, word);
    }

    public async Task<long> CardinalityCheck()
    {
        return await Db.HyperLogLogLengthAsync(_hllName);
    }

    public async Task<string[]> TopKCheck(long top)
    {
        return ((RedisResult[]) await Db.ExecuteAsync("TOPK.LIST", _topKName)).Select(s=>(string)s).ToArray();
    }

    public async Task GetSize(IDictionary<string, long> dictionary)
    {        
        dictionary["bfSize"] = (long) await Db.ExecuteAsync("MEMORY", "USAGE", _bfName);
        dictionary["cmsSize"] = (long) await Db.ExecuteAsync("MEMORY", "USAGE", _cmsName);
        dictionary["hllSize"] = (long) await Db.ExecuteAsync("MEMORY", "USAGE", _hllName);
        dictionary["topKSize"] = (long) await Db.ExecuteAsync("MEMORY", "USAGE", _topKName);
    }
}