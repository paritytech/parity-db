# Review

I did add md paragraph called review and comments starting with [Review].
I also propose some simple changes:
- minor syntactic changes.
- additional comment when it would have save me code reading (I did not try to document all, just what would have help me moving through code while reading).

Key fix to 32 byte:
- index.rs: we only need 8 start byte so most size fits.

Chunk (page) fix to 512 byte (64 entries):
I am not sure if the choice here is related to the page size or the number of entries (with faster search would it be good to use bigger page size).


- TODO a word on salt?? -> in col used for hash


Question: not sure about the use case of 'validate'.

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
Call to commit does not actuall mean data is written.
- commit queue in db.rs does not seem compatible with this
- mmap not having flush guarantee does not either.
- flush worker also defers log flushing
Ok
So the db requires graceful shutdown.
If it does not happen, state can be incorrect.

Ok all is fine as long as all persistence is in a single db, but still would should be strongly underline.
-> makes using a single db (no rocksdb in pkdot) rather prioritary.
(IIUC, behave like rocksdb with 'DBOptions::manual_wal_flush', which can be a gain for rocksdb too (but would require some periodic similar flush call).

The data is in the system at the moment it got flushed in on of the log: append or flush one.
Corresponding to a 'record_id' atomicity (one call to commit), seems ok.

# Implementation

## Data structure
Data is organized into columns. Each column serving a particular type of data, e.g. state or headers. Column consists of an index (up to 255) and a set of 16 value tables for varying value size.  

### Index
Index is an is mmap-backed dynamically sized probing hash table. Each entry is a page of 64 8-byte entries, making 512 bytes. Each 8-byte entry contains 32 bits of value address, 4 bits of value table index and 28 bit value `c` derived from  `k`. `c` is computed by skipping `n` high bits of `k` and taking the next 28 bits. `k` is 256-bit key that is derived from the original key and is uniformly distributed. `n` is current index bit-size. First `n` bits of `k` map `k` to a page. Entries inside the page are unsorted. Empty entry is denoted with a zero value. Empty database starts with `n` = 16, which allows to put just 240 bits of `k` in the value table. 


#### Review Notes

TODO not up to date: value range is not 32 bit but 6 (64 entry per page) + n as there is no more possible value from indexing (from comment).
Leaving 64 - 4 - (6 + n) of hash (initially 38 bit).
Value adress being n + 6 + 4 as we need to cover all entries of similar size case.

To optimize the page search, I could think of : https://event.cwi.nl/damon2009/DaMoN09-KarySearch.pdf.
Probably using 16 (8 seems to small like 256 over 64, 1/4 chance to conflict) bit of our k bits to search first. (then we need to access remaining of page but only at a given entry).

512o 64 address of 64bit so 64 * 8 byte. AKA CHUNK_LEN.
64 entries per chunck aka CHUNK_ENTRIES.
-> but find is done only on less than half: (28 bit value), would make sense to me to pack those, maybe make then 32 bit and reduce value address to 28 bit + 4 bit table.

2^16 -> 65k pages ~ 32Megs ix.

TODO indicates `n` is called `index_bits` in code.
Also currently `n` maximum is 64.
Plus n increment by factor 2: 32m -> 64 -> 128: so 64 seems really enough.

So logic is as much key content as possible to allow early key mismatch detection.
Starting at 2^16 index * 64 = 4M value (2^22 index that needs to be available among all values categories: 26 bit of adressing).
Remaining 38 bit of key.
Considering the key content should at least key 16 bit, then max size for n becomes (64 - 10 - 16) = 38 bit (and 48 bit value address).

About Be and Le usage with u64.
-> key material extracted from hash as BE (first 64 bits). Make senses to me.
-> entry written as LE?? not sure, it puts value index first? BE everywhere seem less error prone.


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

#### Review

TODO duplicate tables, they got different names depending on accesses so it is ok, TODO does it lock write? prev comment indicate not but double querying: insert in new.
TODO check if there may be concurrency issue with the background process??? (not if write is done on previous table: seems ~??).

Question: I did not read all reindex yet, but I wonder about durability, shouldn't reindex be a LogAction? TODO del this question, stupid reindex is happening when enacting log which is last of actions.

## Transaction pipeline
On `commit` all data is first moved to an in-memory overlay, making it available for queries. The commit is then added to the commit queue. This allows for `commit` function to return as early as possible.
Commit queue is processed by a commit worker that collects data that would be modified in the tables and writes it to the available log file. All modified index and value table pages are placed in the in-memory overlay. The file is then handled to another background thread that flushes it to disk and adds it to the finalization queue.
Finally, another thread handles the finalization queue. It reads the file and applies all changes to the tables, clearing the page overlay.

On startup if the log files exists they are validated for corruption and enacted upon the tables.

### Review

'data is first moved to an in-memory overlay' -> is it log 'end_record' ? (begin_record create a writer that end_record flush into in memory.
'The commit is then added to the commit queue' -> 'log.to_file' in 'end_record' : actually append before, but under a lock. -> meaning commit queue is file for 'append'. (the higher index in sort, make sense).
-> at end the append become the source for next step
'Commit queue is processed by a commit worker that collects data that would be modified in the tables and writes it to the available log file' -> this should be working on the two log file that are not use for append: source is flushing dest is reading: fn 'flush_one'.
'The file is then handled to another background thread that flushes it to disk and adds it to the finalization queue' -> fn 'read_next' (use a LogReader).

So three file with same format, ordered by execution: appending -> flushing -> reading
appending receive committed change first: lock on commit of the single writer
reading is writing to backend: lock by backend thread.
flushing is ???: queue to avoid reading locking appending probably actually would make sense if just a tmp ptr to file.

TODO put those on function from log.

TODO unclear why clear value not at same pace -> that is probably when we read an insert index we insert a clear value which in read_end will remove value from in memory overlay: seems inefficient (except to allow atomicity), but what happen when you clear something that was change a second time before read_next call. Excpet if state machine is tightly processed, seems racy at first.


TODO seems interesting to indicate the transaction buffer:
- zero is the db commit overlay
- first the LogWriter overlay, local to the write only thread (not shared).
- second the shared overlay, it is a buffer to allow queueing changes.
- third is the file queuing change (redundant with shared overlay)
- last is the db tables/files.

TODO: think if merging some overlay is doable.

So query goes through shared overlay and file, and if write is enable, query also goes through writer.

Question: I did not imediatly get the purpose of the three files. I am still wondering why we do not use the mmap. Is issue that mmap does not work in case of system crash?
Actually would not make sense as the `write_plan` `complete_plan` do not seems to force flush (so the mmap support crash), so the point is probably performance?

# Potential issues
* Memory mapped IO won't be able to support 32-bit systems once the index grows to 2GB.
* Size amplification. Index grow up to about 50% capacity before rebalance is triggered. Which means about 50% of allocated space is actually used for occupied index entries. Additionally, each value table entry is only partially filled with actual data.




