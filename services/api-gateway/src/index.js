/**
 * API Gateway
 * Central entry point for all microservices with routing, load balancing, and middleware
 */

const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const dotenv = require('dotenv');
const { createProxyMiddleware } = require('http-proxy-middleware');
const rateLimit = require('express-rate-limit');
const { v4: uuidv4 } = require('uuid');
const { initTracing } = require('./utils/tracing');
const { initMetrics } = require('./utils/metrics');
const { ServiceRegistry } = require('./services/serviceRegistry');
const { CircuitBreaker } = require('./services/circuitBreaker');
const { RateLimiter } = require('./services/rateLimiter');
const { createPublicCache } = require('./middleware/publicCache');
const { openApiSpec } = require('./docs/openapi');

// Load environment variables
dotenv.config();

// Initialize tracing and metrics
initTracing('api-gateway');
initMetrics('api-gateway');

// Initialize Express app
const app = express();

// Initialize service registry and circuit breakers
const serviceRegistry = new ServiceRegistry();
const circuitBreakers = new Map();
const rateLimiters = new Map();

// Public (unauthenticated) routes that get dedicated rate limits and caching
const PUBLIC_ROUTE_PREFIXES = ['/stats', '/verify', '/catalog'];

const isPublicRoute = (path) =>
    PUBLIC_ROUTE_PREFIXES.some((prefix) => path === prefix || path.startsWith(`${prefix}/`));

// Middleware
app.use(helmet({
    contentSecurityPolicy: {
        directives: {
            defaultSrc: ["'self'"],
            styleSrc: ["'self'", "'unsafe-inline'"],
            scriptSrc: ["'self'"],
            imgSrc: ["'self'", "data:", "https:"],
        },
    },
}));

app.use(cors({
    origin: process.env.ALLOWED_ORIGINS?.split(',') || '*',
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Request-ID', 'X-API-Key']
}));

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Request ID middleware
app.use((req, res, next) => {
    req.requestId = req.headers['x-request-id'] || uuidv4();
    res.setHeader('x-request-id', req.requestId);
    res.setHeader('x-gateway-version', process.env.GATEWAY_VERSION || '1.0.0');
    next();
});

// Global rate limiting
const globalLimiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 1000, // limit each IP to 1000 requests per windowMs
    message: {
        error: {
            message: 'Too many requests from this IP, please try again later.',
            code: 'RATE_LIMIT_EXCEEDED'
        }
    },
    standardHeaders: true,
    legacyHeaders: false,
});

app.use(globalLimiter);

// Per-IP rate limiting for public (unauthenticated) endpoints.
// Defaults are intentionally tighter than the global limiter since these
// routes are reachable without credentials. Configurable via env.
const publicLimiter = rateLimit({
    windowMs: parseInt(process.env.PUBLIC_RATE_LIMIT_WINDOW_MS, 10) || 60 * 1000, // 1 minute
    max: parseInt(process.env.PUBLIC_RATE_LIMIT_MAX, 10) || 60, // 60 requests per window per IP
    message: {
        error: {
            message: 'Too many requests to this public endpoint, please try again later.',
            code: 'PUBLIC_RATE_LIMIT_EXCEEDED'
        }
    },
    standardHeaders: true,
    legacyHeaders: false,
});

// Redis cache-aside for hot public reads (stats, verify, catalog).
// Falls back to a no-op passthrough when Redis is not configured.
const publicCache = createPublicCache({
    redisUrl: process.env.REDIS_URL,
    ttlSeconds: parseInt(process.env.PUBLIC_CACHE_TTL_SECONDS, 10) || 30,
    keyPrefix: process.env.PUBLIC_CACHE_KEY_PREFIX || 'gateway:public:',
});

app.use((req, res, next) => {
    if (req.method === 'GET' && isPublicRoute(req.path)) {
        return publicLimiter(req, res, () => publicCache(req, res, next));
    }
    next();
});

// OpenAPI 3 spec + Swagger UI, gated to non-production environments.
// Served at /api/docs (UI) and /api/docs/openapi.json (raw spec).
const docsEnabled = process.env.NODE_ENV !== 'production' || process.env.ENABLE_API_DOCS === 'true';

if (docsEnabled) {
    app.get('/api/docs/openapi.json', (req, res) => {
        res.json(openApiSpec);
    });

    app.get('/api/docs', (req, res) => {
        res.set('Content-Type', 'text/html');
        res.send(`<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>API Documentation</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>
        window.onload = function () {
            window.ui = SwaggerUIBundle({
                url: '/api/docs/openapi.json',
                dom_id: '#swagger-ui',
                deepLinking: true,
                presets: [SwaggerUIBundle.presets.apis],
            });
        };
    </script>
</body>
</html>`);
    });
}

// Service discovery and health check
app.use(async (req, res, next) => {
    try {
        // Discover service for the request
        const serviceName = getServiceNameFromPath(req.path);
        
        if (serviceName) {
            const service = await serviceRegistry.getService(serviceName);
            
            if (!service || service.instances.length === 0) {
                return res.status(503).json({
                    error: {
                        message: `Service ${serviceName} is not available`,
                        code: 'SERVICE_UNAVAILABLE'
                    }
                });
            }
            
            req.serviceName = serviceName;
            req.serviceInstance = serviceRegistry.selectInstance(serviceName);
            
            if (!req.serviceInstance) {
                return res.status(503).json({
                    error: {
                        message: `No healthy instances available for service ${serviceName}`,
                        code: 'NO_HEALTHY_INSTANCES'
                    }
                });
            }
        }
        
        next();
    } catch (error) {
        console.error('Service discovery error:', error);
        res.status(500).json({
            error: {
                message: 'Service discovery failed',
                code: 'SERVICE_DISCOVERY_ERROR'
            }
        });
    }
});

// Request logging middleware
app.use((req, res, next) => {
    const startTime = Date.now();
    
    res.on('finish', () => {
        const duration = Date.now() - startTime;
        console.log(`${req.method} ${req.path} - ${res.statusCode} - ${duration}ms - ${req.requestId}`);
        
        // Track metrics
        trackRequestMetrics(req, res, duration);
    });
    
    next();
});

// Health check endpoint
app.get('/health', (req, res) => {
    res.status(200).json({
        status: 'healthy',
        service: 'api-gateway',
        timestamp: new Date().toISOString(),
        version: process.env.GATEWAY_VERSION || '1.0.0',
        services: serviceRegistry.getAllServices()
    });
});

// Readiness check endpoint
app.get('/ready', async (req, res) => {
    try {
        const services = await serviceRegistry.getAllServices();
        const allHealthy = services.every(service => 
            service.instances.some(instance => instance.healthy)
        );
        
        res.status(allHealthy ? 200 : 503).json({
            status: allHealthy ? 'ready' : 'not ready',
            services: services.map(service => ({
                name: service.name,
                healthy: service.instances.some(instance => instance.healthy),
                instances: service.instances.length
            })),
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        res.status(503).json({
            status: 'not ready',
            error: error.message,
            timestamp: new Date().toISOString()
        });
    }
});

// Metrics endpoint
app.get('/metrics', async (req, res) => {
    try {
        const metrics = await getGatewayMetrics();
        res.set('Content-Type', 'text/plain');
        res.send(metrics);
    } catch (error) {
        res.status(500).json({
            error: {
                message: 'Failed to get metrics',
                code: 'METRICS_ERROR'
            }
        });
    }
});

// Service routes with proxy middleware
setupServiceProxies(app);

// Admin routes for service management
app.get('/admin/services', async (req, res) => {
    try {
        const services = await serviceRegistry.getAllServices();
        res.json({
            services,
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        res.status(500).json({
            error: {
                message: 'Failed to get services',
                code: 'SERVICES_ERROR'
            }
        });
    }
});

module.exports = app;
