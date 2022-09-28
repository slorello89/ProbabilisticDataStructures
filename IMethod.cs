namespace ProbabilisticDataStructures;

public interface IMethod
{
    Task Initialize(IEnumerable<string> words);

    Task<bool> PresenceCheck(string word);

    Task<long> ItemCount(string word);

    Task<long> CardinalityCheck();

    Task<string[]> TopKCheck(long top);

    Task GetSize(IDictionary<string, long> dictionary);
    
    string Name { get; }
}