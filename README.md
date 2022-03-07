# A database for the blockchain.

ParityDb is an embedded persistent key-value store optimized for blockchain applications.

## Design considerations

* The database is intended to be used for efficiently storing blockchain state encoded into the Patricia-Merkle trie. Which means most of the keys are fixed size and uniformly distributed. Most values are small. Values over 16 kbytes are rare. Trie nodes may be shared by multiple tries and/or branches, therefore reference counting is required.
* Read performance is more important than write performance for blockchain transaction throughput. Writes are typically performed in large batches, when the new block is imported. There's usually some time between subsequent writes when the blockchain client is idle or executes the block.

### API
The database is a universal key-value storage that supports transactions. The API allows the data to be partitioned into columns. It is recommended that each column contains entries corresponding to a single data type. E.g. state trie node, block headers, blockchain transactions, etc. Two types of column indexes are supported: Hash and Btree.

### Transactions
Database supports multiple concurrent readers. All writes are serialized. Writes are perform in batches, also known as transactions. Transaction are applied atomically. Either all of the transaction data is written, or none. Queries can't retrieve partially committed data.

### No cache
Database does implement any custom data caching. Instead it relies on OS page cache. Performance of a large database therefore depends on how much system memory is available to be used in OS page cache.

### Durability
Database is restored to consistent state if IO is interrupted at any point.

# Implementation details

## Data structure
Each column stores data in a set of 256 value tables, with 255 tables containing entries of certain size range up to 32kbytes limit. The last 256th value table size stores entries that are over 32k split into multiple parts. Hash columns also include a hash index file.

### Metadata
Metadata file contains database definition. This includes a set of columns with configuration specified for each column.

### Hash index
Hash index is an is mmap-backed dynamically sized probing hash table. For each key the index computes a uniformly distributed 256-bit hash 'k'. For index of size `n` first `n` bit of `k` map to the 512 byte index page. Each page is an unordered list of 64 8-byte entries. Each 8-byte entry contains value address and some additional bits of `k`. Empty entry is denoted with a zero value. Empty database starts with `n` = 16.
Value address includes a 8-bit value table index and an index of an entry in that table.
The first 16 kbytes of each index file is used to store statistics for the column.

### Value tables
Value table is linear array of fixed-size entries that can grow as necessary. Each entry may contain one of the following:
  * Filled entry that contains 240 bits of `k`, 15 bit data value size, compression flag, optional reference counter, and the actual value.
  * Tombstone entry. This contains an index of the previous tombstone, forming a linked list of empty entries.
  * Multipart entry. This is much like Filled, additionally holding an address of the next entry that holds continuation of the data.

## Hash index operations.

### Hash index lookup
Compute `k`, find index page using first `n` bits. Search for a matching entry that has matching key bits. Use the address in the entry to query the partial `k` and value from a value table. Confirm that `k` matches expected value.

### Hash index insertion
If an insertion is attempted into a full index page a reindex is triggered. 
Page size of 64 index entries trigger a reindex once load factor reaches about 0.52.

### Reindex
When a collision can't be resolved, a new index table is created with twice the capacity. Insertion is immediately continued to the new table. A background process is started that moves entries from the old table to the new. All queries during that process check both tables.

## BTree index operations.
TODO

## Transaction pipeline
On `commit` all data is moved to an in-memory overlay, making it available for queries. That data is then added to the commit queue. This allows for `commit` function to return as early as possible.
Commit queue is processed by a commit worker that collects data that would be modified in the index or value tables and writes it to the binary log file as a sequence of commands. All modified index and value table pages are placed in the in-memory overlay. The file is then handled to another background thread that flushes it to disk and adds it to the finalization queue.
Finally, another thread handles the finalization queue. It reads the binary log file and applies all changes to the tables, clearing the page overlay.

On startup if any log files exist, they are validated for corruption and enacted upon the tables.

