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
                code: 'GET_SERVICES_ERROR'
            }
        });
    }
});

app.post('/admin/services/:name/instances', async (req, res) => {
    try {
        const { name } = req.params;
        const { url, healthCheckUrl } = req.body;
        
        await serviceRegistry.registerInstance(name, {
            url,
            healthCheckUrl,
            healthy: true,
            registeredAt: new Date()
        });
        
        res.status(201).json({
            message: `Instance registered for service ${name}`,
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        res.status(500).json({
            error: {
                message: 'Failed to register instance',
                code: 'REGISTER_INSTANCE_ERROR'
            }
        });
    }
});

app.delete('/admin/services/:name/instances/:instanceId', async (req, res) => {
    try {
        const { name, instanceId } = req.params;
        
        await serviceRegistry.deregisterInstance(name, instanceId);
        
        res.json({
            message: `Instance ${instanceId} deregistered for service ${name}`,
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        res.status(500).json({
            error: {
                message: 'Failed to deregister instance',
                code: 'DEREGISTER_INSTANCE_ERROR'
            }
        });
    }
});

// Circuit breaker management
app.get('/admin/circuit-breakers', (req, res) => {
    const breakers = Array.from(circuitBreakers.entries()).map(([name, breaker]) => ({
        name,
        state: breaker.getState(),
        stats: breaker.getStats()
    }));
    
    res.json({
        breakers,
        timestamp: new Date().toISOString()
    });
});

app.post('/admin/circuit-breakers/:name/reset', (req, res) => {
    const { name } = req.params;
    const breaker = circuitBreakers.get(name);
    
    if (!breaker) {
        return res.status(404).json({
            error: {
                message: `Circuit breaker ${name} not found`,
                code: 'CIRCUIT_BREAKER_NOT_FOUND'
            }
        });
    }
    
    breaker.reset();
    
    res.json({
        message: `Circuit breaker ${name} reset`,
        timestamp: new Date().toISOString()
    });
});

// Error handling middleware
app.use((err, req, res, next) => {
    console.error('Gateway error:', err);
    
    if (err.code === 'ECONNREFUSED' || err.code === 'ETIMEDOUT') {
        return res.status(503).json({
            error: {
                message: 'Service temporarily unavailable',
                code: 'SERVICE_UNAVAILABLE'
            }
        });
    }
    
    res.status(err.status || 500).json({
        error: {
            message: err.message || 'Internal server error',
            code: err.code || 'INTERNAL_ERROR'
        }
    });
});

// 404 handler
app.use((req, res) => {
    res.status(404).json({
        error: {
            message: 'Route not found',
            code: 'ROUTE_NOT_FOUND'
        }
    });
});

// Helper functions
function getServiceNameFromPath(path) {
    const pathParts = path.split('/').filter(Boolean);
    
    if (pathParts.length === 0) return null;
    
    // Map path prefixes to service names
    const serviceMap = {
        'auth': 'auth-service',
        'users': 'user-service',
        'products': 'product-service',
        'orders': 'order-service',
        'payments': 'payment-service',
        'notifications': 'notification-service',
        'analytics': 'analytics-service',
        'stats': 'analytics-service',
        'verify': 'verification-service',
        'catalog': 'catalog-service'
    };
    
    return serviceMap[pathParts[0]] || null;
}

function setupServiceProxies(app) {
    const services = [
        { name: 'auth-service', path: '/auth' },
        { name: 'user-service', path: '/users' },
        { name: 'product-service', path: '/products' },
        { name: 'order-service', path: '/orders' },
        { name: 'payment-service', path: '/payments' },
        { name: 'notification-service', path: '/notifications' },
        { name: 'analytics-service', path: '/analytics' },
        { name: 'analytics-service', path: '/stats' },
        { name: 'verification-service', path: '/verify' },
        { name: 'catalog-service', path: '/catalog' }
    ];
    
    services.forEach(({ name, path }) => {
        app.use(path, createProxyMiddleware({
            target: process.env[`${name.toUpperCase().replace(/-/g, '_')}_URL`] || `http://localhost:${getServicePort(name)}`,
            changeOrigin: true,
            pathRewrite: {
                [`^${path}`]: ''
            },
            onProxyReq: (proxyReq, req, res) => {
                // Add request ID to proxied requests
                proxyReq.setHeader('x-request-id', req.requestId);
                
                // Add service instance info
                if (req.serviceInstance) {
                    proxyReq.setHeader('x-service-instance', req.serviceInstance.id);
                }
            },
            onError: (err, req, res) => {
                console.error(`Proxy error for ${name}:`, err);
                
                if (!res.headersSent) {
                    res.status(503).json({
                        error: {
                            message: `Service ${name} is unavailable`,
                            code: 'SERVICE_UNAVAILABLE'
                        }
                    });
                }
            }
        }));
    });
}

function getServicePort(serviceName) {
    const ports = {
        'auth-service': 3001,
        'user-service': 3002,
        'product-service': 3003,
        'order-service': 3004,
        'payment-service': 3005,
        'notification-service': 3006,
        'analytics-service': 3007,
        'verification-service': 3008,
        'catalog-service': 3009
    };
    
    return ports[serviceName] || 3000;
}

function trackRequestMetrics(req, res, duration) {
    // Metrics tracking implementation
    // This would integrate with your metrics system
}

async function getGatewayMetrics() {
    // Metrics retrieval implementation
    // This would integrate with your metrics system
    return '';
}

// Start server
const PORT = process.env.PORT || 8080;

const server = app.listen(PORT, () => {
    console.log(`API Gateway listening on port ${PORT}`);
    console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('SIGTERM received, shutting down gracefully');
    server.close(() => {
        console.log('Server closed');
        process.exit(0);
    });
});

process.on('SIGINT', () => {
    console.log('SIGINT received, shutting down gracefully');
    server.close(() => {
        console.log('Server closed');
        process.exit(0);
    });
});

module.exports = app;
