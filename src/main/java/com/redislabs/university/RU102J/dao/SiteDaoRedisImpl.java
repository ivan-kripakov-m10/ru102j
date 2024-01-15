package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.api.Site;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;

public class SiteDaoRedisImpl implements SiteDao {
    private static final int DEFAULT_COUNT = 1000;
    private final JedisPool jedisPool;

    public SiteDaoRedisImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    // When we insert a site, we set all of its values into a single hash.
    // We then store the site's id in a set for easy access.
    @Override
    public void insert(Site site) {
        try (Jedis jedis = jedisPool.getResource()) {
            String hashKey = RedisSchema.getSiteHashKey(site.getId());
            String siteIdKey = RedisSchema.getSiteIDsKey();
            jedis.hmset(hashKey, site.toMap());
            jedis.sadd(siteIdKey, hashKey);
        }
    }

    @Override
    public Site findById(long id) {
        try(Jedis jedis = jedisPool.getResource()) {
            String key = RedisSchema.getSiteHashKey(id);
            return findSiteByKey(jedis, key);
        }
    }

    // Challenge #1

    @Override
    public Set<Site> findAll() {
        // START Challenge #1
        try (Jedis jedis = jedisPool.getResource()) {
            String siteIdKey = RedisSchema.getSiteIDsKey();
            Set<Site> allSites = new HashSet<>();
            String cursor = SCAN_POINTER_START;
            do {
                ScanResult<String> scanResult = jedis.sscan(
                    siteIdKey,
                    cursor,
                    new ScanParams().count(DEFAULT_COUNT).match("*")
                );
                scanResult.getResult().stream()
                    .map(siteKey -> findSiteByKey(jedis ,siteKey))
                    .filter(Objects::nonNull)
                    .forEach(allSites::add);
                cursor = scanResult.getCursor();
            } while (!cursor.equals(SCAN_POINTER_START));

            return allSites;
        }
        // END Challenge #1
    }

    private Site findSiteByKey(Jedis jedis, String siteKey) {
        Map<String, String> fields = jedis.hgetAll(siteKey);
        if (fields == null || fields.isEmpty()) {
            return null;
        } else {
            return new Site(fields);
        }
    }
}
