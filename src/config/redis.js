const redis = require('redis');
const logger = require('../utils/logger');

class RedisConfig {
  constructor() {
    this.client = null;
    this.publisher = null;
    this.subscriber = null;
    this.isConnected = false;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 10;
    this.reconnectDelay = 5000; // 5 seconds
  }

  async initialize() {
    try {
      // Create Redis client with configuration
      const redisConfig = {
        host: process.env.REDIS_HOST || 'localhost',
        port: process.env.REDIS_PORT || 6379,
        password: process.env.REDIS_PASSWORD || undefined,
        db: process.env.REDIS_DB || 0,
        retryDelayOnFailover: 100,
        enableReadyCheck: true,
        maxRetriesPerRequest: 3,
        lazyConnect: true,
        keepAlive: 30000,
        connectTimeout: 10000,
        commandTimeout: 5000,
        // Offline queue
        enableOfflineQueue: true,
        // Reconnect strategy
        retryStrategy: (times) => {
          if (times >= this.maxReconnectAttempts) {
            logger.error('Redis max reconnection attempts reached');
            return null;
          }
          const delay = Math.min(times * this.reconnectDelay, 30000);
          logger.warn(`Redis reconnecting in ${delay}ms (attempt ${times})`);
          return delay;
        }
      };

      // Main client for operations
      this.client = redis.createClient(redisConfig);

      // Publisher for events
      this.publisher = redis.createClient(redisConfig);

      // Subscriber for listening to events
      this.subscriber = redis.createClient(redisConfig);

      // Set up event handlers
      this.setupEventHandlers();

      // Connect all clients
      await Promise.all([
        this.client.connect(),
        this.publisher.connect(),
        this.subscriber.connect()
      ]);

      this.isConnected = true;
      this.reconnectAttempts = 0;
      
      logger.info('Redis connected successfully');
      return true;

    } catch (error) {
      logger.error('Failed to connect to Redis:', error);
      this.isConnected = false;
      throw error;
    }
  }

  setupEventHandlers() {
    // Main client events
    this.client.on('connect', () => {
      logger.info('Redis client connecting...');
    });

    this.client.on('ready', () => {
      logger.info('Redis client ready');
      this.isConnected = true;
      this.reconnectAttempts = 0;
    });

    this.client.on('error', (error) => {
      logger.error('Redis client error:', error);
      this.isConnected = false;
    });

    this.client.on('end', () => {
      logger.warn('Redis client connection ended');
      this.isConnected = false;
    });

    this.client.on('reconnecting', () => {
      this.reconnectAttempts++;
      logger.info(`Redis reconnecting... (attempt ${this.reconnectAttempts})`);
    });

    // Publisher events
    this.publisher.on('error', (error) => {
      logger.error('Redis publisher error:', error);
    });

    // Subscriber events
    this.subscriber.on('error', (error) => {
      logger.error('Redis subscriber error:', error);
    });

    this.subscriber.on('message', (channel, message) => {
      logger.debug(`Redis message received on channel ${channel}:`, message);
    });
  }

  // Cache-aside helpers for hot public reads (stats, verify, catalog)
  async getCache(key) {
    if (!this.isConnected) {
      return null;
    }

    try {
      const cached = await this.client.get(key);
      return cached ? JSON.parse(cached) : null;
    } catch (error) {
      logger.error(`Failed to read cache key ${key}:`, error);
      return null;
    }
  }

  async setCache(key, value, ttlSeconds) {
    if (!this.isConnected) {
      return false;
    }

    try {
      const ttl = Number(ttlSeconds) > 0 ? Number(ttlSeconds) : 60;
      await this.client.set(key, JSON.stringify(value), { EX: ttl });
      return true;
    } catch (error) {
      logger.error(`Failed to write cache key ${key}:`, error);
      return false;
    }
  }

  async delCache(key) {
    if (!this.isConnected) {
      return false;
    }

    try {
      await this.client.del(key);
      return true;
    } catch (error) {
      logger.error(`Failed to delete cache key ${key}:`, error);
      return false;
    }
  }

  // Per-IP rate limiting for public/unauthenticated routes
  async checkRateLimit(identifier, limit, windowSeconds) {
    if (!this.isConnected) {
      // Fail open when Redis is unavailable so public routes stay reachable
      return { allowed: true, remaining: limit, resetAt: Date.now() + windowSeconds * 1000 };
    }

    const max = Number(limit) > 0 ? Number(limit) : 60;
    const window = Number(windowSeconds) > 0 ? Number(windowSeconds) : 60;
    const key = `ratelimit:${identifier}`;

    try {
      const count = await this.client.incr(key);
      if (count === 1) {
        await this.client.expire(key, window);
      }

      const ttl = await this.client.ttl(key);
      const resetAt = Date.now() + (ttl > 0 ? ttl : window) * 1000;

      return {
        allowed: count <= max,
        remaining: Math.max(0, max - count),
        resetAt
      };
    } catch (error) {
      logger.error(`Failed to check rate limit for ${identifier}:`, error);
      return { allowed: true, remaining: max, resetAt: Date.now() + window * 1000 };
    }
  }

  // Queue operations
  async enqueue(queueName, data, options = {}) {
    if (!this.isConnected) {
      throw new Error('Redis not connected');
    }

    try {
      const priority = options.priority || 0;
      const delay = options.delay || 0;
      const id = options.id || this.generateId();

      const item = {
        id,
        data,
        priority,
        enqueuedAt: Date.now(),
        delay,
        retryCount: 0,
        maxRetries: options.maxRetries || 3
      };

      if (delay > 0) {
        // Add to delayed queue
        const score = Date.now() + delay;
        await this.client.zAdd(`${queueName}:delayed`, { score, value: JSON.stringify(item) });
      } else {
        // Add to immediate queue
        await this.client.lPush(`${queueName}:pending`, JSON.stringify(item));
      }

      logger.debug(`Enqueued item ${id} to queue ${queueName}`);
      return id;

    } catch (error) {
      logger.error(`Failed to enqueue to queue ${queueName}:`, error);
      throw error;
    }
  }

  async dequeue(queueName, timeout = 30) {
    if (!this.isConnected) {
      throw new Error('Redis not connected');
    }

    try {
      // Process delayed items first
      await this.processDelayedItems(queueName);

      // Block and wait for item from pending queue
      const result = await this.client.brPop(`${queueName}:pending`, timeout);
      
      if (result) {
        const item = JSON.parse(result.element);
        logger.debug(`Dequeued item ${item.id} from queue ${queueName}`);
        return item;
      }

      return null;

    } catch (error) {
      logger.error(`Failed to dequeue from queue ${queueName}:`, error);
      throw error;
    }
  }

  async processDelayedItems(queueName) {
    try {
      const now = Date.now();
      const items = await this.client.zRangeByScore(`${queueName}:delayed`, 0, now);

      if (items.length > 0) {
        // Move items from delayed to pending queue
        const pipeline = this.client.multi();
        
        for (const item of items) {
          pipeline.lPush(`${queueName}:pending`, item);
          pipeline.zRem(`${queueName}:delayed`, item);
        }

        await pipeline.exec();
        logger.debug(`Moved ${items.length} delayed items to pending queue ${queueName}`);
      }

    } catch (error) {
      logger.error(`Failed to process delayed items for queue ${queueName}:`, error);
    }
  }

  async requeueForRetry(queueName, item, delay = null) {
    if (!this.isConnected) {
      throw new Error('Redis not connected');
    }

    try {
      item.retryCount += 1;
      
      // Calculate exponential backoff delay if not provided
      if (delay === null) {
        delay = Math.min(Math.pow(2, item.retryCount) * 1000, 30000); // Max 30 seconds
      }

      const score = Date.now() + delay;
      await this.client.zAdd(`${queueName}:delayed`, { score, value: JSON.stringify(item) });
      
      logger.debug(`Requeued item ${item.id} for retry in ${delay}ms (attempt ${item.retryCount})`);
      return true;

    } catch (error) {
      logger.error(`Failed to requeue item ${item.id}:`, error);
      throw error;
    }
  }

  async moveToFailed(queueName, item, error) {
    if (!this.isConnected) {
      throw new Error('Redis not connected');
    }

    try {
      const failedItem = {
        ...item,
        failedAt: Date.now(),
        error: error.message || error
      };

      await this.client.lPush(`${queueName}:failed`, JSON.stringify(failedItem));
      logger.debug(`Moved item ${item.id} to failed queue:`, error);
      return true;

    } catch (error) {
      logger.error(`Failed to move item ${item.id} to failed queue:`, error);
      throw error;
    }
  }

  async getQueueStats(queueName) {
    if (!this.isConnected) {
      throw new Error('Redis not connected');
    }

    try {
      const [pending, delayed, failed] = await Promise.all([
        this.client.lLen(`${queueName}:pending`),
        this.client.zCard(`${queueName}:delayed`),
        this.client.lLen(`${queueName}:failed`)
      ]);

      return {
        pending,
        delayed,
        failed,
        total: pending + delayed + failed
      };

    } catch (error) {
      logger.error(`Failed to get queue stats for ${queueName}:`, error);
      throw error;
    }
  }

  async clearQueue(queueName) {
    if (!this.isConnected) {
      throw new Error('Redis not connected');
    }

    try {
      await Promise.all([
        this.client.del(`${queueName}:pending`),
        this.client.del(`${queueName}:delayed`),
        this.client.del(`${queueName}:failed`)
      ]);
      logger.debug(`Cleared queue ${queueName}`);
      return true;
    } catch (error) {
      logger.error(`Failed to clear queue ${queueName}:`, error);
      throw error;
    }
  }

  generateId() {
    return `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  }

  async disconnect() {
    try {
      await Promise.all([
        this.client && this.client.quit(),
        this.publisher && this.publisher.quit(),
        this.subscriber && this.subscriber.quit()
      ]);
      this.isConnected = false;
      logger.info('Redis disconnected');
    } catch (error) {
      logger.error('Failed to disconnect Redis:', error);
    }
  }
}

module.exports = new RedisConfig();
