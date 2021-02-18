# A database for the blockchain.

## **WARNING: PartyDB is still in development and should not be used in production. Use at your own risk.**

## Design considerations

### API
The database is a universal key-value storage that supports transactions. It does not support iteration or prefix-based retrieval.

### State-optimized
90% Of blockchain data and IO is trie nodes. Database should allow for efficient storage and retrieval of state data first.

### Single writer
Database should be able to support multiple concurrent readers. It is sufficient to allow a single concurrent writer.

### No cache
Low level LRU caching of blockchain data, such as individual trie nodes, proves to be inefficient. Cache should be done on a higher level of abstractions. I.e. storage items or block headers.

### Transaction isolation
Transaction are applied atomically. Queries can't retrieve partially committed data.

### Durability
Database should be restored to consistent state if IO is interrupted at any point. 

# Implementation

## Data structure
Data is organized into columns. Each column serving a particular type of data, e.g. state or headers. Column consists of an index and a set of 16 value tables for varying value size.  

### Index
Index is an is mmap-backed dynamically sized probing hash table. Each entry is a page of 64 8-byte entries, making 512 bytes.  Each 64-bit entry contains 32 bits of value address, 4 bits of value table index and 28 bit value `c` derived from  `k`. `c` is computed by skipping `n` high bits of `k` and taking the next 28 bits.  `k` is 256-bit key that is derived from the original key and is uniformly distributed. `n` is current index bit-size. First `n` bits of `k` map `k` to a page. Entries inside the page are unsorted. Empty entry is denoted with a zero value. Empty database starts with `n` = 16, which allows to put just 240 bits of `k` in the value table. 

### Value tables
Value table is linear array of fixed-size entries that can grow as necessary. Each entry may contain one of the following:
  * Filled entry, that contains 240 bits of `k`, 16 bit data value size and the actual value
  * Tombstone entry. This contains an index of the previous tombstone, forming a linked list of available entries.
  * Multipart entry. This is much like Filled, additionally holding an address of the next entry that holds continuation of the data.

15 of 16 value tables only allow values up to entry size. An additional table with 8kb entry size is designated for large values and allows multipart entries.

## Operations

### Lookup
Compute `k`, find index page using first `n` bits. Search for a matching entry that has matching `c` bits. Use the address in the entry to query the partial `k`  and value from a value table. Confirm that `k` is indeed what is expected.

### Insertion
If an insertion is attempted into a full index page a reindex is triggered. 
Page size of 64 index entries trigger a reindex once load factor reaches about 50%. 

### Reindex
When a collision can't be resolved, a new table is created with twice the capacity. Insertion is immediately continued to the new table. A background process is started that moves entries from the old table to the new. All queries during that process check both tables.

## Transaction pipeline
On `commit` all data is first moved to an in-memory overlay, making it available for queries. The commit is then added to the commit queue. This allows for `commit` function to return as early as possible.
Commit queue is processed by a commit worker that collects data that would be modified in the tables and writes it to the available log file. All modified index and value table pages are placed in the in-memory overlay. The file is then handled to another background thread that flushes it to disk and adds it to the finalization queue.
Finally, another thread handles the finalization queue. It reads the file and applies all changes to the tables, clearing the page overlay.

On startup if the log files exists they are validated for corruption and enacted upon the tables.

# Potential issues
* Memory mapped IO won't be able to support 32-bit systems once the index grows to 2GB.
* Size amplification. Index grow up to about 50% capacity before rebalance is triggered. Which means about 50% of allocated space is actually used for occupied index entries. Additionally, each value table entry is only partially filled with actual data.




