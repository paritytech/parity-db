# A database suitable for storing blockchain data.

## Design considerations

### API

The database is a universal key-value storage that supports transactions. It does not support iteration or prefix-based retrieval.

### State-optimized

90% Of blockchain data and IO is trie nodes. Database should allow for efficient storage and retrieval of state data first.

### Single writer

Database should be able to support multiple concurrent readers. It is sufficient to support a single concurrent writer.

### No cache

Low level LRU caching of blockchain data, such as individual trie nodes, proves to be inefficient. Cache should be done on a higher level of abstrations. I.e. storage items or block headers.

### Transaction isolation

Transaction are applied atomically. Queries can't retrieve partially committed data.

### Durability

Database should be restored to consistent state if IO is interrupted. 

## Implementation

# Data structure
Data is organised into columns. Each column serving a particular type of data, e.g. state or headers. Each column consists of 4 tables for varying value sizes: up to 128, 256, 512, 4096 bytes and an additional storage for large values (blobs).

`Table` is a is mmmap-backed dynamically sized hash table. Each entry has a fixed size and stores 32 byte `bkey`, 2 byte value size and a value. `bkey` is derived from the original key and is uniformly distributed. Index is first `n` bits of `bkey` where `n` is the current table size.
Table entry sizes should be aligned to IO page boundaries.
When retrieving data from the column, each table is checked in order for matching entries. Since majority of state data is small, this requires checking 1.5-1.8 tables on average for each access. 

# Blob storage

#TODO

# Collision handling

If a table slot is occupied on insertion, an entry may be placed in one of the following slots, but no further than 32 slots ahead. The table maintains that the keys are always increasing, which means that up to 31 entries may be shifted right to make place for a new entry. If there's no free space in the following 32 slots, a rebalance is triggered. On deletion the following entries that are out of their designated slot are shifted left.
Allowing up to 32 out-of-place entries for collision resolution triggers rebalance when the table is about 50% full. Average seek will read 2 entries when the table is close to the rebalance limit. And since the additional reads are sequential they are likely to be prefetched.

# Rebalance

When a collision can't be resolved, a new table is created with twice the capacity. Insertion is immediately continued to the new table. A background process is started that moves entries from the old table to the new. All queries during that process check both tables.

# Write-ahead log.

To support durability a write-ahead log is utilised. On commit all changed data is written to the log first, marking the end of each transaction with a checksum. A background process flushes the log to disk and enacts it upon the column tables. While the data is being flushed it may be retrieved from an in-memory overlay. If the overlay grows too large or the flushing queue can't keep up, insertions are blocked.

On startup if the log file exists it is first checked for corruption. Transactions are enacted up to the first checksum  failure.

### Potential issues

* Memory mapped IO won't be able to support 32-bit systems.
* Size amplification. Hash tables grow up to about 50% capacity before rebalance is triggered. Which means about 50% of allocated space is actually used for occupied table entries. Additionally, each entry is only partially filled with actual data.




