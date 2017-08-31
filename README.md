# kylin-redis

Jedis 详细介绍
Jedis 是 Redis 官方首选的 Java 客户端开发包。

实例方法：

import redis.clients.jedis.*
Jedis jedis = new Jedis("localhost");
jedis.set("foo", "bar");
String value = jedis.get("foo");
支持的特效：

Sorting

Connection handling

Commands operating on any kind of values

Commands operating on string values

Commands operating on hashes

Commands operating on lists

Commands operating on sets

Commands operating on sorted sets

Transactions

Pipelining

Publish/Subscribe

Persistence control commands

Remote server control commands

Connection pooling

Sharding (MD5, MurmurHash)

Key-tags for sharding

Sharding with pipelining

Scripting with pipelining

Maven:

    redis.clients    jedis    2.0.0    jar    compile
