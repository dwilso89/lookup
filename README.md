# lookup
 
## Connectors

### Current
[BloomMapFile](https://hadoop.apache.org/docs/r2.10.0/api/org/apache/hadoop/io/BloomMapFile.html
) - Hadoop's bloom filter and mapfile key-value capability 

[MPH](https://github.com/indeedeng/mph-table) - Indeed's Minimal Perfect Hash immutable key/value store

[PalDB](https://github.com/linkedin/PalDB) - LinkedIn's embeddable write-once key-value store

Simple - Java HashMap implementation for small data sets

### Planned
DynamoDB - Amazon

## Servers
REST - Rapidoid

TCP - Netty

## Filters

Filters can be used to increase look up performance. Servers can be configured to use specific filters during exists and get requests while clients can download filters to help reduce the number of required network requests to the server.

### Bloom Filters
[Guava](https://github.com/google/guava/blob/master/guava/src/com/google/common/hash/BloomFilter.java) - Google's Guava implementation

[Scala](https://github.com/alexandrnikitin/bloom-filter-scala) - Custom scala implementation which "unlimited" element membership

### Planned
Key Set Filter - Set of all keys in the data. Potentially useful for testing and small data sets

Cuckoo Filter - https://en.wikipedia.org/wiki/Cuckoo_filter

Xor Fitler - https://arxiv.org/pdf/1912.08258.pdf | https://lemire.me/blog/2019/12/19/xor-filters-faster-and-smaller-than-bloom-filters/