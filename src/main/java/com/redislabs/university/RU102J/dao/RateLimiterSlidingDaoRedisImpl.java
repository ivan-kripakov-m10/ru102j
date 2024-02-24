package com.redislabs.university.RU102J.dao;

import java.util.UUID;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        // START CHALLENGE #7
        String key = RedisSchema.getSlidingRateLimiterKey(name, windowSizeMS, maxHits);
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.watch(key);
            try (Transaction transaction = jedis.multi()) {
                long now = System.currentTimeMillis();
                transaction.zadd(key, now, UUID.randomUUID().toString());
                transaction.zremrangeByScore(key, 0, now - windowSizeMS);
                transaction.exec();
            }
            long hits = jedis.zcard(key);
            if (hits > maxHits) {
                throw new RateLimitExceededException();
            }
        }
        // END CHALLENGE #7
    }
}
