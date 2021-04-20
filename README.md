# Review

## Markdow

under 'Review' titles.

## Rust

in comment containing `[Review]`

## changes

I also did propose some simple changes:
- minor syntactic changes.
- additional comment when it would have save me code reading (I did not try to document all, just what would have help me moving through code while reading).

## Global question

### Value key stoarge.
store whole 32 byte

### Key type

- index.rs: we only need 8 start byte so most size fits.

### Index

Chunk (page) size to 512 byte (64 entries):
I am not sure if the choice here is related to the page size or the number of entries
(with faster search would it be good to use bigger page size).

### Hash

Not really sure why we use salt option.

### Validate

not sure about the use case of 'validate', seems only to restart from crash.

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

#### Review

I firstly misunderstood 'Durability' (IO failing not breaking db) for some consitency (call to commit means data is written in db).
So there is many irrelevant notes in code.
Call to commit does not actualy mean data is written/flushed to disk.
- commit queue in db.rs only write to in memory queue.
- mmap not having flush guarantee, this is something I am not really confident in.
- flush worker also defers log flushing to file.

So the db requires graceful shutdown.
If it does not happen, state can be incorrect.


About 'commit' semantic, this is a bit more awkward.
All should be fine as long as all persistence is in a single db, but we should be strongly underline
that commit does not flush to disk.
So if process is sigkilled, the state would be consistent (but if another persistence source is used, it could be
in a different state than this db).

If full system is kill, mmap could not have flush: is it problematic?

-> using a single db (no rocksdb in pkdot) seems rather prioritary.
(IIUC, current commit semantic behave like rocksdb with 'DBOptions::manual_wal_flush')
(which can be a gain for rocksdb too (but would require some periodic similar flush call).

# Implementation

## Data structure
Data is organized into columns. Each column serving a particular type of data, e.g. state or headers. Column consists of an index (up to 255) and a set of 16 value tables for varying value size.  

### Index
Index is an is mmap-backed dynamically sized probing hash table. Each entry is a page of 64 8-byte entries, making 512 bytes. Each 8-byte entry contains 32 bits of value address, 4 bits of value table index and 28 bit value `c` derived from  `k`. `c` is computed by skipping `n` high bits of `k` and taking the next 28 bits. `k` is 256-bit key that is derived from the original key and is uniformly distributed. `n` is current index bit-size. First `n` bits of `k` map `k` to a page. Entries inside the page are unsorted. Empty entry is denoted with a zero value. Empty database starts with `n` = 16, which allows to put just 240 bits of `k` in the value table. 


#### Review Notes

TODO looks outdated: value range is not 32 bit but really the remaining size.
Leaving 64 - 4 - (6 + n) of hash (initially 38 bit).
Value adress being n + 6 + 4 as we need to cover all entries of similar size case.

To optimize the page search, I could think of : https://event.cwi.nl/damon2009/DaMoN09-KarySearch.pdf.
Probably using 16 (8 seems to small like 256 over 64, 1/4 chance to conflict) bit of our k bits at start of page or a first search.
(then we need to access remaining of page but only at a given entry).

2^16 -> 65k pages ~ 32Megs ix size per column initially.

TODO indicates `n` is called `index_bits` in code.
Also currently `n` maximum is 64 - 10. (but we would want some at least 16 key length so would say 64 - 26 = 38 bit).
Even 38 bit seems really enough.

So logic is as much key content as possible to allow early key mismatch detection.
Starting at 2^16 index * 64 = 4M value (2^22 index that needs to be available among all values categories: 26 bit of adressing).
Remaining 38 bit of key.
Considering the key content should at least key 16 bit, then max size for n becomes (64 - 10 - 16) = 38 bit (and 48 bit value address).

About Be and Le usage with u64.
-> key material extracted from hash as BE (first 64 bits). Make senses to me.
-> entry written as LE?? seems like all code defaults to LE.


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

### Review

TODO seems interesting to indicate the transaction buffer:
- zero is the db commit overlay
- first the LogWriter overlay, local to the write only thread (not shared).
- second the shared overlay, it is a buffer to allow queueing changes.
- third is the file queuing change (redundant with shared overlay)
- last is the db tables/files.

So query goes through shared overlay and file, and if write is enable, query also goes through writer.

# Potential issues
* Memory mapped IO won't be able to support 32-bit systems once the index grows to 2GB.
* Size amplification. Index grow up to about 50% capacity before rebalance is triggered. Which means about 50% of allocated space is actually used for occupied index entries. Additionally, each value table entry is only partially filled with actual data.
