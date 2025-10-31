const express = require('express');
const cors = require('cors');
const ShopifyClient = require('./src/shopifyClient');
const logger = require('./src/logger');
require('dotenv').config();

class ShopifyStatsServer {
    constructor() {
        this.app = express();
        this.port = 3001;
        this.shopifyClient = new ShopifyClient();
        this.cache = new Map();
        this.cacheExpiry = 5 * 60 * 1000; // 5 minutes cache
        
        this.setupMiddleware();
        this.setupRoutes();
    }

    setupMiddleware() {
        this.app.use(cors());
        this.app.use(express.json());
        
        // Request logging middleware
        this.app.use((req, res, next) => {
            logger.info(`${req.method} ${req.path} - ${req.ip}`);
            next();
        });
    }

    setupRoutes() {
        // Health check endpoint
        this.app.get('/health', (req, res) => {
            res.json({ status: 'OK', timestamp: new Date().toISOString() });
        });

        // Get shop information
        this.app.get('/api/shop/info', async (req, res) => {
            try {
                const shopInfo = await this.getShopInfo();
                res.json({
                    success: true,
                    data: shopInfo
                });
            } catch (error) {
                logger.error('Error fetching shop info:', error.message);
                res.status(500).json({
                    success: false,
                    error: error.message
                });
            }
        });

        // Get products statistics
        this.app.get('/api/products/stats', async (req, res) => {
            try {
                const stats = await this.getProductsStats();
                res.json({
                    success: true,
                    data: stats
                });
            } catch (error) {
                logger.error('Error fetching product stats:', error.message);
                res.status(500).json({
                    success: false,
                    error: error.message
                });
            }
        });

        // Get detailed products list with pagination
        this.app.get('/api/products/list', async (req, res) => {
            try {
                const page = parseInt(req.query.page) || 1;
                const limit = parseInt(req.query.limit) || 20;
                const status = req.query.status; // 'active', 'draft', 'archived'
                
                const products = await this.getProductsList(page, limit, status);
                res.json({
                    success: true,
                    data: products
                });
            } catch (error) {
                logger.error('Error fetching products list:', error.message);
                res.status(500).json({
                    success: false,
                    error: error.message
                });
            }
        });

        // Get categories/product types statistics
        this.app.get('/api/categories/stats', async (req, res) => {
            try {
                const stats = await this.getCategoriesStats();
                res.json({
                    success: true,
                    data: stats
                });
            } catch (error) {
                logger.error('Error fetching categories stats:', error.message);
                res.status(500).json({
                    success: false,
                    error: error.message
                });
            }
        });

        // Get inventory statistics
        this.app.get('/api/inventory/stats', async (req, res) => {
            try {
                const stats = await this.getInventoryStats();
                res.json({
                    success: true,
                    data: stats
                });
            } catch (error) {
                logger.error('Error fetching inventory stats:', error.message);
                res.status(500).json({
                    success: false,
                    error: error.message
                });
            }
        });

        // Get pricing statistics
        this.app.get('/api/pricing/stats', async (req, res) => {
            try {
                const stats = await this.getPricingStats();
                res.json({
                    success: true,
                    data: stats
                });
            } catch (error) {
                logger.error('Error fetching pricing stats:', error.message);
                res.status(500).json({
                    success: false,
                    error: error.message
                });
            }
        });

        // Get recent products (created in last X days)
        this.app.get('/api/products/recent', async (req, res) => {
            try {
                const days = parseInt(req.query.days) || 7;
                const recentProducts = await this.getRecentProducts(days);
                res.json({
                    success: true,
                    data: recentProducts
                });
            } catch (error) {
                logger.error('Error fetching recent products:', error.message);
                res.status(500).json({
                    success: false,
                    error: error.message
                });
            }
        });

        // Get products by vendor
        this.app.get('/api/products/by-vendor', async (req, res) => {
            try {
                const vendorStats = await this.getProductsByVendor();
                res.json({
                    success: true,
                    data: vendorStats
                });
            } catch (error) {
                logger.error('Error fetching vendor stats:', error.message);
                res.status(500).json({
                    success: false,
                    error: error.message
                });
            }
        });

        // Search products
        this.app.get('/api/products/search', async (req, res) => {
            try {
                const query = req.query.q;
                const limit = parseInt(req.query.limit) || 10;
                
                if (!query) {
                    return res.status(400).json({
                        success: false,
                        error: 'Search query is required'
                    });
                }
                
                const results = await this.searchProducts(query, limit);
                res.json({
                    success: true,
                    data: results
                });
            } catch (error) {
                logger.error('Error searching products:', error.message);
                res.status(500).json({
                    success: false,
                    error: error.message
                });
            }
        });

        // Clear cache endpoint
        this.app.post('/api/cache/clear', (req, res) => {
            this.cache.clear();
            logger.info('Cache cleared manually');
            res.json({
                success: true,
                message: 'Cache cleared successfully'
            });
        });

        // Get all endpoints documentation
        this.app.get('/', (req, res) => {
            res.json({
                message: 'Shopify Stats API Server',
                version: '1.0.0',
                endpoints: {
                    '/health': 'Health check',
                    '/api/shop/info': 'Get shop information',
                    '/api/products/stats': 'Get products statistics',
                    '/api/products/list': 'Get products list (paginated)',
                    '/api/products/recent': 'Get recent products (query: days)',
                    '/api/products/by-vendor': 'Get products grouped by vendor',
                    '/api/products/search': 'Search products (query: q, limit)',
                    '/api/categories/stats': 'Get categories/product types statistics',
                    '/api/inventory/stats': 'Get inventory statistics',
                    '/api/pricing/stats': 'Get pricing statistics',
                    '/api/cache/clear': 'Clear server cache (POST)'
                },
                parameters: {
                    '/api/products/list': '?page=1&limit=20&status=active',
                    '/api/products/recent': '?days=7',
                    '/api/products/search': '?q=necklace&limit=10'
                }
            });
        });
    }

    // Cache helper methods
    getCacheKey(key) {
        return `stats_${key}`;
    }

    getCachedData(key) {
        const cacheKey = this.getCacheKey(key);
        const cached = this.cache.get(cacheKey);
        
        if (cached && (Date.now() - cached.timestamp) < this.cacheExpiry) {
            logger.debug(`Cache hit for key: ${key}`);
            return cached.data;
        }
        
        return null;
    }

    setCachedData(key, data) {
        const cacheKey = this.getCacheKey(key);
        this.cache.set(cacheKey, {
            data,
            timestamp: Date.now()
        });
        logger.debug(`Cache set for key: ${key}`);
    }

    // Shop information
    async getShopInfo() {
        const cacheKey = 'shop_info';
        let cached = this.getCachedData(cacheKey);
        if (cached) return cached;

        await this.shopifyClient.initialize();
        const shopInfo = await this.shopifyClient.testConnection();
        
        this.setCachedData(cacheKey, shopInfo);
        return shopInfo;
    }

    // Products statistics
    async getProductsStats() {
        const cacheKey = 'products_stats';
        let cached = this.getCachedData(cacheKey);
        if (cached) return cached;

        logger.info('Fetching products statistics...');
        const products = await this.shopifyClient.getAllProducts();
        
        const stats = {
            total: products.length,
            active: products.filter(p => p.status === 'active').length,
            draft: products.filter(p => p.status === 'draft').length,
            archived: products.filter(p => p.status === 'archived').length,
            published: products.filter(p => p.published_at).length,
            unpublished: products.filter(p => !p.published_at).length,
            withImages: products.filter(p => p.images && p.images.length > 0).length,
            withoutImages: products.filter(p => !p.images || p.images.length === 0).length,
            totalVariants: products.reduce((sum, p) => sum + (p.variants ? p.variants.length : 0), 0),
            avgVariantsPerProduct: products.length > 0 ? 
                (products.reduce((sum, p) => sum + (p.variants ? p.variants.length : 0), 0) / products.length).toFixed(2) : 0
        };

        this.setCachedData(cacheKey, stats);
        return stats;
    }

    // Products list with pagination
    async getProductsList(page = 1, limit = 20, status = null) {
        logger.info(`Fetching products list - Page: ${page}, Limit: ${limit}, Status: ${status}`);
        
        const products = await this.shopifyClient.getAllProducts();
        
        // Filter by status if provided
        let filteredProducts = products;
        if (status) {
            filteredProducts = products.filter(p => p.status === status);
        }

        // Sort by creation date (newest first)
        filteredProducts.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));

        // Paginate
        const startIndex = (page - 1) * limit;
        const endIndex = startIndex + limit;
        const paginatedProducts = filteredProducts.slice(startIndex, endIndex);

        // Transform products to include only essential information
        const transformedProducts = paginatedProducts.map(product => ({
            id: product.id,
            title: product.title,
            handle: product.handle,
            status: product.status,
            created_at: product.created_at,
            updated_at: product.updated_at,
            published_at: product.published_at,
            vendor: product.vendor,
            product_type: product.product_type,
            tags: product.tags,
            variants_count: product.variants ? product.variants.length : 0,
            images_count: product.images ? product.images.length : 0,
            price_range: this.getProductPriceRange(product)
        }));

        return {
            products: transformedProducts,
            pagination: {
                page,
                limit,
                total: filteredProducts.length,
                totalPages: Math.ceil(filteredProducts.length / limit),
                hasNext: endIndex < filteredProducts.length,
                hasPrev: page > 1
            },
            filters: { status }
        };
    }

    // Categories statistics
    async getCategoriesStats() {
        const cacheKey = 'categories_stats';
        let cached = this.getCachedData(cacheKey);
        if (cached) return cached;

        logger.info('Fetching categories statistics...');
        const products = await this.shopifyClient.getAllProducts();
        
        const productTypes = {};
        const vendors = {};
        
        products.forEach(product => {
            // Product types
            const productType = product.product_type || 'Uncategorized';
            productTypes[productType] = (productTypes[productType] || 0) + 1;
            
            // Vendors
            const vendor = product.vendor || 'Unknown';
            vendors[vendor] = (vendors[vendor] || 0) + 1;
        });

        const stats = {
            productTypes: {
                total: Object.keys(productTypes).length,
                distribution: Object.entries(productTypes)
                    .sort(([,a], [,b]) => b - a)
                    .reduce((acc, [type, count]) => {
                        acc[type] = count;
                        return acc;
                    }, {})
            },
            vendors: {
                total: Object.keys(vendors).length,
                distribution: Object.entries(vendors)
                    .sort(([,a], [,b]) => b - a)
                    .reduce((acc, [vendor, count]) => {
                        acc[vendor] = count;
                        return acc;
                    }, {})
            }
        };

        this.setCachedData(cacheKey, stats);
        return stats;
    }

    // Inventory statistics
    async getInventoryStats() {
        const cacheKey = 'inventory_stats';
        let cached = this.getCachedData(cacheKey);
        if (cached) return cached;

        logger.info('Fetching inventory statistics...');
        const products = await this.shopifyClient.getAllProducts();
        
        let totalInventory = 0;
        let outOfStock = 0;
        let lowStock = 0; // Less than 5 items
        let inStock = 0;
        let trackedVariants = 0;
        let untrackedVariants = 0;

        products.forEach(product => {
            if (product.variants) {
                product.variants.forEach(variant => {
                    if (variant.inventory_management) {
                        trackedVariants++;
                        const quantity = variant.inventory_quantity || 0;
                        totalInventory += quantity;
                        
                        if (quantity === 0) {
                            outOfStock++;
                        } else if (quantity < 5) {
                            lowStock++;
                        } else {
                            inStock++;
                        }
                    } else {
                        untrackedVariants++;
                    }
                });
            }
        });

        const stats = {
            totalInventoryCount: totalInventory,
            trackedVariants,
            untrackedVariants,
            inStock,
            lowStock,
            outOfStock,
            averageInventoryPerProduct: products.length > 0 ? 
                (totalInventory / products.length).toFixed(2) : 0
        };

        this.setCachedData(cacheKey, stats);
        return stats;
    }

    // Pricing statistics
    async getPricingStats() {
        const cacheKey = 'pricing_stats';
        let cached = this.getCachedData(cacheKey);
        if (cached) return cached;

        logger.info('Fetching pricing statistics...');
        const products = await this.shopifyClient.getAllProducts();
        
        const prices = [];
        let withCompareAtPrice = 0;
        let freeProducts = 0;

        products.forEach(product => {
            if (product.variants) {
                product.variants.forEach(variant => {
                    const price = parseFloat(variant.price) || 0;
                    prices.push(price);
                    
                    if (price === 0) freeProducts++;
                    if (variant.compare_at_price) withCompareAtPrice++;
                });
            }
        });

        prices.sort((a, b) => a - b);

        const stats = {
            totalVariants: prices.length,
            averagePrice: prices.length > 0 ? 
                (prices.reduce((sum, price) => sum + price, 0) / prices.length).toFixed(2) : 0,
            medianPrice: prices.length > 0 ? 
                prices[Math.floor(prices.length / 2)].toFixed(2) : 0,
            minPrice: prices.length > 0 ? prices[0].toFixed(2) : 0,
            maxPrice: prices.length > 0 ? prices[prices.length - 1].toFixed(2) : 0,
            freeProducts,
            withCompareAtPrice,
            priceRanges: {
                'Under $50': prices.filter(p => p < 50).length,
                '$50 - $100': prices.filter(p => p >= 50 && p < 100).length,
                '$100 - $500': prices.filter(p => p >= 100 && p < 500).length,
                '$500 - $1000': prices.filter(p => p >= 500 && p < 1000).length,
                'Over $1000': prices.filter(p => p >= 1000).length
            }
        };

        this.setCachedData(cacheKey, stats);
        return stats;
    }

    // Recent products
    async getRecentProducts(days = 7) {
        logger.info(`Fetching products created in last ${days} days...`);
        const products = await this.shopifyClient.getAllProducts();
        
        const cutoffDate = new Date();
        cutoffDate.setDate(cutoffDate.getDate() - days);

        const recentProducts = products
            .filter(product => new Date(product.created_at) > cutoffDate)
            .sort((a, b) => new Date(b.created_at) - new Date(a.created_at))
            .map(product => ({
                id: product.id,
                title: product.title,
                status: product.status,
                created_at: product.created_at,
                vendor: product.vendor,
                product_type: product.product_type,
                price_range: this.getProductPriceRange(product)
            }));

        return {
            count: recentProducts.length,
            products: recentProducts,
            period: `${days} days`,
            cutoffDate: cutoffDate.toISOString()
        };
    }

    // Products by vendor
    async getProductsByVendor() {
        const cacheKey = 'products_by_vendor';
        let cached = this.getCachedData(cacheKey);
        if (cached) return cached;

        logger.info('Fetching products by vendor...');
        const products = await this.shopifyClient.getAllProducts();
        
        const vendorStats = {};
        
        products.forEach(product => {
            const vendor = product.vendor || 'Unknown';
            if (!vendorStats[vendor]) {
                vendorStats[vendor] = {
                    productCount: 0,
                    active: 0,
                    draft: 0,
                    archived: 0,
                    productTypes: new Set()
                };
            }
            
            vendorStats[vendor].productCount++;
            vendorStats[vendor][product.status]++;
            if (product.product_type) {
                vendorStats[vendor].productTypes.add(product.product_type);
            }
        });

        // Convert Sets to Arrays for JSON serialization
        Object.keys(vendorStats).forEach(vendor => {
            vendorStats[vendor].productTypes = Array.from(vendorStats[vendor].productTypes);
            vendorStats[vendor].uniqueProductTypes = vendorStats[vendor].productTypes.length;
        });

        // Sort by product count
        const sortedVendors = Object.entries(vendorStats)
            .sort(([,a], [,b]) => b.productCount - a.productCount)
            .reduce((acc, [vendor, stats]) => {
                acc[vendor] = stats;
                return acc;
            }, {});

        this.setCachedData(cacheKey, sortedVendors);
        return sortedVendors;
    }

    // Search products
    async searchProducts(query, limit = 10) {
        logger.info(`Searching products for: "${query}"`);
        const products = await this.shopifyClient.getAllProducts();
        
        const searchQuery = query.toLowerCase();
        const matchingProducts = products
            .filter(product => {
                return product.title.toLowerCase().includes(searchQuery) ||
                       (product.body_html && product.body_html.toLowerCase().includes(searchQuery)) ||
                       (product.tags && product.tags.toLowerCase().includes(searchQuery)) ||
                       (product.vendor && product.vendor.toLowerCase().includes(searchQuery)) ||
                       (product.product_type && product.product_type.toLowerCase().includes(searchQuery));
            })
            .slice(0, limit)
            .map(product => ({
                id: product.id,
                title: product.title,
                handle: product.handle,
                status: product.status,
                vendor: product.vendor,
                product_type: product.product_type,
                tags: product.tags,
                created_at: product.created_at,
                price_range: this.getProductPriceRange(product)
            }));

        return {
            query: query,
            count: matchingProducts.length,
            products: matchingProducts
        };
    }

    // Helper method to get product price range
    getProductPriceRange(product) {
        if (!product.variants || product.variants.length === 0) {
            return { min: '0.00', max: '0.00' };
        }

        const prices = product.variants
            .map(variant => parseFloat(variant.price) || 0)
            .sort((a, b) => a - b);

        return {
            min: prices[0].toFixed(2),
            max: prices[prices.length - 1].toFixed(2)
        };
    }

    // Start the server
    async start() {
        try {
            // Test Shopify connection on startup
            await this.shopifyClient.initialize();
            logger.info('Shopify connection verified');

            this.app.listen(this.port, () => {
                logger.info(`🚀 Shopify Stats Server running on port ${this.port}`);
                logger.info(`📊 Dashboard available at: http://localhost:${this.port}`);
                logger.info(`🔗 API endpoints available at: http://localhost:${this.port}/api/*`);
            });
        } catch (error) {
            logger.error('Failed to start server:', error.message);
            process.exit(1);
        }
    }
}

// Create and start the server
const server = new ShopifyStatsServer();

// Handle graceful shutdown
process.on('SIGINT', () => {
    logger.info('Shutting down server...');
    process.exit(0);
});

process.on('SIGTERM', () => {
    logger.info('Shutting down server...');
    process.exit(0);
});

// Start the server
if (require.main === module) {
    server.start().catch(error => {
        logger.error('Failed to start server:', error);
        process.exit(1);
    });
}

module.exports = ShopifyStatsServer;