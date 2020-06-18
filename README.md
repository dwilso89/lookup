# lookup
 
A simple application for accessing data using Key/Value-based API.

Includes various connectors to proxy queries to external services as well as hosting prebuilt data sets.
 
## Connectors

### Included
[BloomMapFile](https://hadoop.apache.org/docs/r2.10.0/api/org/apache/hadoop/io/BloomMapFile.html
) - Hadoop's bloom filter and mapfile key-value capability 

[MPH](https://github.com/indeedeng/mph-table) - Indeed's Minimal Perfect Hash immutable key/value store

[PalDB](https://github.com/linkedin/PalDB) - LinkedIn's embeddable write-once key-value store

Simple - Java HashMap implementation for small data sets

### Planned
DynamoDB - Amazon

## Servers

### Included
HTTP - Rapidoid based HTTP/REST

TCP - Netty based simple TCP server for existence queries

### Planned
RMI - Java inter-process communication client/server

## Membership Filters

Filters can be used to increase look up performance. Servers can be configured to use a filter during existence queries and get requests while clients can download filters to help reduce network requests to the server.

### Included
[Hadoop](https://hadoop.apache.org/docs/r2.10.0/api/org/apache/hadoop/util/bloom/DynamicBloomFilter.html) - Implementation used as part of Hadoop BloomMapFile

[Guava](https://github.com/google/guava/blob/master/guava/src/com/google/common/hash/BloomFilter.java) - Google's Guava BloomFilter implementation

[Scala](https://github.com/alexandrnikitin/bloom-filter-scala) - Custom scala BloomFilter implementation with "unlimited" element membership

[FastFilter](https://github.com/FastFilter/fastfilter_java) - A library of membership filter implementations including Xor, Bloom, and Cuckoo

Key Set Filter - Java HashSet of all keys in the data. Useful for testing and small data sets

## Examples

See dist/examples/README.md