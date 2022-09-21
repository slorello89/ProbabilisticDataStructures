# Probabilistic Data Structures

Hello there! This repository contains some demonstration code and comparisons for answering discrete problems you might encounter in your day to day work using both brute force methods in SQL and Redis, and using Probabilistic Data Structures in Redis. The problems we seek to evaluate are the following:

1. Set Presence - Has an item been added to a set?
2. Count of Items in a Stream - How many times has a given item been added in a stream?
3. Heavy Hitters in a Stream - what are the most frequent items added to a stream
4. Set Cardinality - How many unique items have been added to a set.

In this example, we break down the individual words in Herman Melville's Epic - "Moby Dick" and insert them into 4 data sources.

1. An Unindexed Postgres table
2. An Indexed Postgres table
3. A Redis [Sorted Set](https://redis.io/docs/data-types/sorted-sets/)
4. A set of Redis Probabilistic Data Structures Including a Bloom Filter, a TopK, a Count-Min-Sketch, and a HyperLogLog.

## Prereqs

* The [.NET 6 SDK](https://dotnet.microsoft.com/en-us/download/dotnet/6.0)
* Some means of running [Redis Stack](https://redis.io/docs/stack/) and Postgres (preferably with Docker)

## Start Docker

Use the following commands to start Redis and Postgres

### Redis

```bash
docker run -d -p 6379:6379 -p 8001:8001 redis/redis-stack
```

### Postgres

```bash
 docker run -d \
	-e POSTGRES_PASSWORD=secretpassword \
	-e PGDATA=/var/lib/postgresql/data/pgdata \
	-v ~/mount/postgres:/var/lib/postgresql/data \
    -p 5432:5432 \
	postgres
```


## Run the app

To run the App, simply run `dotnet run`