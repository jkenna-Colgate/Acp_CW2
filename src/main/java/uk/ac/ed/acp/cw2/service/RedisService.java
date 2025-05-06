package uk.ac.ed.acp.cw2.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
@Service
public class RedisService {

    private final StringRedisTemplate redisTemplate;
    private static final Logger logger = LoggerFactory.getLogger(RedisService.class);

    public RedisService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public Integer getVersion(String key) {
        String val = redisTemplate.opsForValue().get(key);
        logger.debug("getVersion called for key='{}', returning '{}'", key, val);
        return (val != null) ? Integer.parseInt(val) : null;
    }

    public void setVersion(String key, int version) {
        redisTemplate.opsForValue().set(key, String.valueOf(version));
        logger.debug("setVersion called for key='{}', setting to '{}'", key, version);
    }

    public void deleteKey(String key) {
        redisTemplate.delete(key);
    }
    public void clearAll() {
        redisTemplate.getConnectionFactory().getConnection().flushDb();
    }

}
