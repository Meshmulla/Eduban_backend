# Services

Backend services for the platform. Public (unauthenticated) endpoints are
protected by per-IP rate limiting and accelerated with a Redis cache-aside
layer.

## Public endpoints

The following routes are unauthenticated and therefore rate limited and cached:

| Route      | Description                          |
| ---------- | ------------------------------------ |
| `stats`    | Aggregate platform statistics        |
| `verify`   | Public verification lookups          |
| `catalog`  | Public catalog listing               |

### Rate limiting

Each public route is limited per client IP. Limits use a fixed window and
return `429 Too Many Requests` with a `Retry-After` header once exceeded.

| Env var                    | Default | Description                                        |
| -------------------------- | ------- | -------------------------------------------------- |
| `RATE_LIMIT_WINDOW_SECONDS`| `60`    | Length of the rate limit window, in seconds.       |
| `RATE_LIMIT_MAX_REQUESTS`  | `60`    | Max requests per IP per window for public routes.  |

### Caching

Hot public reads use a Redis cache-aside strategy: the handler checks Redis
first, falls back to the origin on a miss, then populates the cache. Cache
entries expire after the configured TTL.

| Env var                | Default | Description                                  |
| ---------------------- | ------- | -------------------------------------------- |
| `REDIS_URL`            | —       | Redis connection string used for caching.    |
| `CACHE_TTL_SECONDS`    | `30`    | TTL for cached public reads, in seconds.     |

When `REDIS_URL` is unset, caching is disabled and requests pass through to
the origin.

## Configuration

Copy `.env.example` to `.env` and adjust the values above as needed. All
settings are optional and fall back to the defaults listed in the tables.
