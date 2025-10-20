const axios = require('axios');
const logger = require('./logger');

// Load environment variables
require('dotenv').config();

class ShopifyClient {
  constructor() {
    this.baseURL = null;
    this.headers = null;
    this.initialized = false;

    // Shopify category mapping using proper GID format for GraphQL
    this.categoryMap = new Map([
      // Jewelry > Anklets
      ['anklets', 'gid://shopify/TaxonomyCategory/aa-6-1'],
      ['anklet', 'gid://shopify/TaxonomyCategory/aa-6-1'],

      // Jewelry > Body Jewelry
      ['body jewelry', 'gid://shopify/TaxonomyCategory/aa-6-2'],
      ['body', 'gid://shopify/TaxonomyCategory/aa-6-2'],

      // Jewelry > Bracelets
      ['bracelets', 'gid://shopify/TaxonomyCategory/aa-6-3'],
      ['bracelet', 'gid://shopify/TaxonomyCategory/aa-6-3'],

      // Jewelry > Brooches & Lapel Pins
      ['brooches', 'gid://shopify/TaxonomyCategory/aa-6-4'],
      ['brooch', 'gid://shopify/TaxonomyCategory/aa-6-4'],
      ['lapel pins', 'gid://shopify/TaxonomyCategory/aa-6-4'],
      ['lapel pin', 'gid://shopify/TaxonomyCategory/aa-6-4'],

      // Jewelry > Charms & Pendants
      ['charms', 'gid://shopify/TaxonomyCategory/aa-6-5'],
      ['charm', 'gid://shopify/TaxonomyCategory/aa-6-5'],
      ['pendants', 'gid://shopify/TaxonomyCategory/aa-6-5'],
      ['pendant', 'gid://shopify/TaxonomyCategory/aa-6-5'],

      // Jewelry > Earrings
      ['earrings', 'gid://shopify/TaxonomyCategory/aa-6-6'],
      ['earring', 'gid://shopify/TaxonomyCategory/aa-6-6'],

      // Jewelry > Jewelry Sets
      ['jewelry sets', 'gid://shopify/TaxonomyCategory/aa-6-7'],
      ['jewelry set', 'gid://shopify/TaxonomyCategory/aa-6-7'],
      ['sets', 'gid://shopify/TaxonomyCategory/aa-6-7'],
      ['set', 'gid://shopify/TaxonomyCategory/aa-6-7'],

      // Jewelry > Necklaces
      ['necklaces', 'gid://shopify/TaxonomyCategory/aa-6-8'],
      ['necklace', 'gid://shopify/TaxonomyCategory/aa-6-8'],
      ['chain necklaces', 'gid://shopify/TaxonomyCategory/aa-6-8'],
      ['chain necklace', 'gid://shopify/TaxonomyCategory/aa-6-8'],

      // Jewelry > Rings
      ['rings', 'gid://shopify/TaxonomyCategory/aa-6-9'],
      ['ring', 'gid://shopify/TaxonomyCategory/aa-6-9'],

      // Jewelry > Smart Watches
      ['smart watches', 'gid://shopify/TaxonomyCategory/aa-6-12'],
      ['smart watch', 'gid://shopify/TaxonomyCategory/aa-6-12'],

      // Jewelry > Watch Accessories
      ['watch accessories', 'gid://shopify/TaxonomyCategory/aa-6-10'],
      ['watch accessory', 'gid://shopify/TaxonomyCategory/aa-6-10'],

      // Jewelry > Watch Accessories > Watch Bands
      ['watch bands', 'gid://shopify/TaxonomyCategory/aa-6-10-1'],
      ['watch band', 'gid://shopify/TaxonomyCategory/aa-6-10-1'],

      // Jewelry > Watch Accessories > Watch Stickers & Decals
      ['watch stickers', 'gid://shopify/TaxonomyCategory/aa-6-10-2'],
      ['watch decals', 'gid://shopify/TaxonomyCategory/aa-6-10-2'],
      ['watch stickers & decals', 'gid://shopify/TaxonomyCategory/aa-6-10-2'],

      // Jewelry > Watch Accessories > Watch Winders
      ['watch winders', 'gid://shopify/TaxonomyCategory/aa-6-10-3'],
      ['watch winder', 'gid://shopify/TaxonomyCategory/aa-6-10-3'],

      // Jewelry > Watches
      ['watches', 'gid://shopify/TaxonomyCategory/aa-6-11'],
      ['watch', 'gid://shopify/TaxonomyCategory/aa-6-11'],

      // Additional mappings for chain types that should be under necklaces
      ['chains', 'gid://shopify/TaxonomyCategory/aa-6-8'],
      ['chain', 'gid://shopify/TaxonomyCategory/aa-6-8'],
      ['rope chains', 'gid://shopify/TaxonomyCategory/aa-6-8'],
      ['rope chain', 'gid://shopify/TaxonomyCategory/aa-6-8'],
      ['curb chains', 'gid://shopify/TaxonomyCategory/aa-6-8'],
      ['curb chain', 'gid://shopify/TaxonomyCategory/aa-6-8'],
      ['cable chains', 'gid://shopify/TaxonomyCategory/aa-6-8'],
      ['cable chain', 'gid://shopify/TaxonomyCategory/aa-6-8']
    ]);
    
    // Default category for jewelry if no specific match is found
    this.defaultJewelryCategory = 'gid://shopify/TaxonomyCategory/aa-6-8'; // Necklaces as default
  }

  async initialize() {
    try {
      if (!process.env.SHOPIFY_STORE_URL || !process.env.SHOPIFY_ACCESS_TOKEN) {
        throw new Error('Missing Shopify configuration. Please check SHOPIFY_STORE_URL and SHOPIFY_ACCESS_TOKEN in .env file');
      }

      // Ensure store URL has the correct format
      let storeUrl = process.env.SHOPIFY_STORE_URL;
      if (!storeUrl.includes('.myshopify.com')) {
        storeUrl = storeUrl.replace('.com', '.myshopify.com');
      }
      if (!storeUrl.startsWith('https://')) {
        storeUrl = `https://${storeUrl}`;
      }

      this.baseURL = `${storeUrl}/admin/api/2025-10`;
      this.headers = {
        'X-Shopify-Access-Token': process.env.SHOPIFY_ACCESS_TOKEN,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      };

      // Test the connection
      await this.testConnection();

      this.initialized = true;
      logger.info('Shopify client initialized successfully');
      logger.info(`Connected to: ${storeUrl}`);
    } catch (error) {
      logger.error('Failed to initialize Shopify client:', error.message);
      throw error;
    }
  }

  async testConnection() {
    try {
      const response = await axios.get(`${this.baseURL}/shop.json`, { headers: this.headers });
      logger.info(`Connected to shop: ${response.data.shop.name}`);
      
      // Test GraphQL endpoint as well
      await this.testGraphQLConnection();
      
      return response.data.shop;
    } catch (error) {
      if (error.response) {
        logger.error(`Shopify API Error: ${error.response.status} - ${error.response.statusText}`);
        if (error.response.status === 401) {
          throw new Error('Invalid Shopify access token. Please check your SHOPIFY_ACCESS_TOKEN.');
        }
        if (error.response.status === 404) {
          throw new Error('Shop not found. Please check your SHOPIFY_STORE_URL.');
        }
        throw new Error(`API Error: ${error.response.status} - ${error.response.data?.errors || error.response.statusText}`);
      }
      throw error;
    }
  }

  /**
   * Test GraphQL connection
   */
  async testGraphQLConnection() {
    try {
      const query = `
        query {
          shop {
            name
            id
          }
        }
      `;

      const response = await axios.post(`${this.baseURL}/graphql.json`, {
        query: query
      }, { headers: this.headers });

      if (response.data.errors) {
        throw new Error(`GraphQL errors: ${JSON.stringify(response.data.errors)}`);
      }

      logger.info(`✓ GraphQL connection successful: ${response.data.data.shop.name}`);
      return response.data.data.shop;

    } catch (error) {
      logger.error('GraphQL connection test failed:', error.message);
      throw error;
    }
  }

  async createProduct(productData) {
    if (!this.initialized) {
      await this.initialize();
    }

    try {
      // Log category information for debugging
      if (productData.product_category_id) {
        logger.debug(`Creating product with category ID: ${productData.product_category_id}`);
      } else {
        logger.debug('Creating product without category ID');
      }

      const response = await axios.post(`${this.baseURL}/products.json`, {
        product: productData
      }, { headers: this.headers });

      const product = response.data.product;
      logger.info(`Created product: ${product.title} (ID: ${product.id})`);
      
      // Update category using GraphQL if category is specified
      if (productData.category) {
        try {
          await this.updateProductCategory(product.id, productData.category);
          logger.info(`✓ Updated product category: ${productData.category}`);
        } catch (categoryError) {
          logger.warn(`Failed to update category for product ${product.id}:`, categoryError.message);
          // Don't throw error as product was created successfully
        }
      }
      
      return product;
    } catch (error) {
      logger.error('Error creating product in Shopify:', error.message);
      if (error.response && error.response.data) {
        logger.error('Shopify API Error Details:', JSON.stringify(error.response.data, null, 2));
      }
      throw error;
    }
  }

  /**
   * Update product category using GraphQL API
   * @param {number} productId - The product ID from Shopify
   * @param {string} categoryId - The category ID to assign
   */
  async updateProductCategory(productId, categoryId) {
    if (!this.initialized) {
      await this.initialize();
    }

    try {
      // Convert product ID to GraphQL GID format
      const productGid = `gid://shopify/Product/${productId}`;
      
      // GraphQL mutation to update product category
      const mutation = `
        mutation productUpdate($product: ProductUpdateInput!) {
          productUpdate(product: $product) {
            product {
              id
              title
              category {
                id
                name
              }
            }
            userErrors {
              field
              message
            }
          }
        }
      `;

      const variables = {
        product: {
          id: productGid,
          category: categoryId
        }
      };

      const response = await axios.post(`${this.baseURL}/graphql.json`, {
        query: mutation,
        variables: variables
      }, { headers: this.headers });

      if (response.data.errors) {
        throw new Error(`GraphQL errors: ${JSON.stringify(response.data.errors)}`);
      }

      const result = response.data.data.productUpdate;
      
      if (result.userErrors && result.userErrors.length > 0) {
        const errors = result.userErrors.map(error => `${error.field}: ${error.message}`).join(', ');
        throw new Error(`User errors: ${errors}`);
      }

      logger.debug(`Successfully updated category for product ${productId}: ${categoryId}`);
      return result.product;

    } catch (error) {
      logger.error('Error updating product category via GraphQL:', error.message);
      if (error.response && error.response.data) {
        logger.error('GraphQL API Error Details:', JSON.stringify(error.response.data, null, 2));
      }
      throw error;
    }
  }

  /**
   * Get all existing products from Shopify (for duplicate checking)
   */
  async getAllProducts() {
    if (!this.initialized) {
      await this.initialize();
    }

    try {
      const allProducts = [];
      let nextPageUrl = `${this.baseURL}/products.json?limit=250`;

      while (nextPageUrl) {
        logger.debug(`Fetching products: ${nextPageUrl}`);
        
        const response = await axios.get(nextPageUrl, { headers: this.headers });
        const products = response.data.products;
        
        allProducts.push(...products);
        
        // Check for next page
        const linkHeader = response.headers.link;
        nextPageUrl = this.extractNextPageUrl(linkHeader);
        
        // Small delay to respect rate limits
        if (nextPageUrl) {
          await this.delay(200);
        }
      }

      logger.info(`Fetched ${allProducts.length} existing products from Shopify`);
      return allProducts;

    } catch (error) {
      logger.error('Error fetching products from Shopify:', error.message);
      if (error.response?.status === 429) {
        logger.warn('Rate limited - using empty product list for this run');
        return [];
      }
      throw error;
    }
  }

  // Helper method to extract next page URL from Link header
  extractNextPageUrl(linkHeader) {
    if (!linkHeader) return null;

    const links = linkHeader.split(',');
    for (const link of links) {
      if (link.includes('rel="next"')) {
        const match = link.match(/<([^>]+)>/);
        return match ? match[1] : null;
      }
    }
    return null;
  }

  // Helper method to add delay
  delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  // Process a single product from CSV (for testing/limited processing)
  async processSingleProductFromCSV(csvItem, options = {}) {
    if (!this.initialized) {
      await this.initialize();
    }

    const { dryRun = false } = options;

    logger.info(`=== PROCESSING SINGLE PRODUCT ===`);
    logger.info(`Product: ${csvItem.Description || csvItem.Item}`);
    logger.info(`SKU: ${csvItem.Item}`);
    logger.info(`Dry run: ${dryRun}`);

    try {
      if (dryRun) {
        logger.info('DRY RUN: Would create product with data:');
        const productData = this.mapCSVToShopifyProduct(csvItem);
        logger.info(JSON.stringify(productData, null, 2));
        return { success: true, dryRun: true, productData };
      } else {
        const result = await this.processCreateItem(csvItem, false);
        logger.info('Successfully processed single product');
        return { success: true, result };
      }
    } catch (error) {
      logger.error('Error processing single product:', error.message);
      return { success: false, error: error.message };
    }
  }

  /**
   * Process a batch of products directly (simplified for batch processing)
   */
  async processBatchDirect(csvItems, options = {}) {
    if (!this.initialized) {
      await this.initialize();
    }

    const { dryRun = false } = options;

    const results = {
      total: csvItems.length,
      created: 0,
      errors: 0,
      errorDetails: []
    };

    logger.debug(`Processing batch of ${csvItems.length} products directly`);

    const promises = csvItems.map(async (csvItem) => {
      try {
        if (dryRun) {
          logger.debug(`[DRY RUN] Would create product: ${csvItem.Description || csvItem.Item}`);
          return { success: true, type: 'dry-run', item: csvItem.Item };
        } else {
          const productData = this.mapCSVToShopifyProduct(csvItem);
          const createdProduct = await this.createProduct(productData);
          logger.debug(`✓ Created: ${createdProduct.title} (ID: ${createdProduct.id})`);
          return { success: true, type: 'created', product: createdProduct, item: csvItem.Item };
        }
      } catch (error) {
        logger.error(`✗ Failed to create product ${csvItem.Item || csvItem.Description}:`, error.message);
        return {
          success: false,
          error: error.message,
          item: csvItem.Item || 'Unknown'
        };
      }
    });

    // Execute all promises in parallel
    const batchResults = await Promise.allSettled(promises);

    // Process results
    batchResults.forEach((result, index) => {
      if (result.status === 'fulfilled') {
        const value = result.value;
        if (value.success) {
          results.created++;
        } else {
          results.errors++;
          results.errorDetails.push({
            item: value.item,
            error: value.error
          });
        }
      } else {
        results.errors++;
        results.errorDetails.push({
          item: csvItems[index]?.Item || 'Unknown',
          error: result.reason?.message || 'Promise rejected'
        });
      }
    });

    logger.debug(`Batch completed: ${results.created} created, ${results.errors} errors`);
    return results;
  }

  // Process CSV products: Create new ones and update existing ones with changes
  async processProductsFromCSV(csvData, shopifyProducts, options = {}) {
    if (!this.initialized) {
      await this.initialize();
    }

    const {
      batchSize = 10,
      delayBetweenBatches = 1000,
      dryRun = false,
      parallelBatch = true,
      enableUpdates = true // New option to enable/disable updates
    } = options;

    logger.info(`=== STARTING SHOPIFY PRODUCT PROCESSING ===`);
    logger.info(`Total CSV items: ${csvData.length}`);
    logger.info(`Batch size: ${batchSize}`);
    logger.info(`Parallel batch processing: ${parallelBatch}`);
    logger.info(`Enable updates: ${enableUpdates}`);
    logger.info(`Delay between batches: ${delayBetweenBatches}ms`);
    logger.info(`Dry run: ${dryRun}`);
    logger.info('='.repeat(50));

    const results = {
      total: csvData.length,
      created: 0,
      updated: 0,
      skipped: 0,
      errors: 0,
      createdProducts: [],
      updatedProducts: [],
      errorDetails: []
    };

    // Use provided Shopify products or fetch them
    let existingProducts = shopifyProducts || [];
    if (!shopifyProducts) {
      logger.info('Fetching existing products from Shopify...');
      existingProducts = await this.getAllProducts();
    }
    logger.info(`Found ${existingProducts.length} existing products in Shopify`);

    // Create lookup maps for faster product matching
    const existingProductsBySku = new Map();
    const existingProductsByTitle = new Map();

    existingProducts.forEach(product => {
      // Index by title
      existingProductsByTitle.set(product.title.toLowerCase(), product);

      // Index by SKU from variants
      if (product.variants) {
        product.variants.forEach(variant => {
          if (variant.sku) {
            existingProductsBySku.set(variant.sku.toLowerCase(), product);
          }
        });
      }
    });

    logger.info(`Built lookup index: ${existingProductsBySku.size} SKUs, ${existingProductsByTitle.size} titles`);

    // Categorize CSV items into create/update batches
    const { productsToCreate, productsToUpdate } = this.categorizeProducts(
      csvData,
      existingProductsBySku,
      existingProductsByTitle,
      enableUpdates
    );

    logger.info(`Products to create: ${productsToCreate.length}`);
    logger.info(`Products to update: ${productsToUpdate.length}`);
    logger.info(`Total to process: ${productsToCreate.length + productsToUpdate.length}`);

    const allProductsToProcess = [
      ...productsToCreate.map(item => ({ ...item, action: 'create' })),
      ...productsToUpdate.map(item => ({ ...item, action: 'update' }))
    ];

    // Process in batches
    for (let i = 0; i < allProductsToProcess.length; i += batchSize) {
      const batch = allProductsToProcess.slice(i, i + batchSize);
      const batchNumber = Math.floor(i / batchSize) + 1;
      const totalBatches = Math.ceil(allProductsToProcess.length / batchSize);

      logger.info(`Processing batch ${batchNumber}/${totalBatches} (${batch.length} items)`);

      if (parallelBatch && !dryRun) {
        // Process batch items in parallel using Promise.all
        await this.processBatchParallelWithUpdates(batch, results, dryRun);
      } else {
        // Process batch items sequentially
        await this.processBatchSequentialWithUpdates(batch, results, dryRun);
      }

      // Delay between batches (except for the last batch)
      if (i + batchSize < allProductsToProcess.length) {
        logger.info(`Waiting ${delayBetweenBatches}ms before next batch...`);
        await this.delay(delayBetweenBatches);
      }
    }

    // Log final results
    logger.info('='.repeat(50));
    logger.info(`=== PRODUCT PROCESSING COMPLETE ===`);
    logger.info(`Total processed: ${results.total}`);
    logger.info(`Successfully created: ${results.created}`);
    logger.info(`Successfully updated: ${results.updated}`);
    logger.info(`Skipped (no changes): ${results.skipped}`);
    logger.info(`Errors: ${results.errors}`);

    if (results.errors > 0) {
      logger.info('\nError details:');
      results.errorDetails.slice(0, 10).forEach((detail, index) => {
        logger.error(`${index + 1}. ${detail.csvItem}: ${detail.error}`);
      });
      if (results.errorDetails.length > 10) {
        logger.info(`... and ${results.errorDetails.length - 10} more errors`);
      }
    }

    logger.info('='.repeat(50));
    return results;
  }

  // Categorize CSV products into create/update batches
  categorizeProducts(csvData, existingProductsBySku, existingProductsByTitle, enableUpdates) {
    const productsToCreate = [];
    const productsToUpdate = [];

    csvData.forEach(csvItem => {
      const sku = csvItem.Item?.toLowerCase();
      const title = (csvItem.Description || csvItem.Item || '').toLowerCase();

      // Try to find existing product by SKU first, then by title
      let existingProduct = null;
      if (sku && existingProductsBySku.has(sku)) {
        existingProduct = existingProductsBySku.get(sku);
      } else if (title && existingProductsByTitle.has(title)) {
        existingProduct = existingProductsByTitle.get(title);
      }

      if (existingProduct) {
        if (enableUpdates) {
          // Check if product needs updates
          const needsUpdate = this.doesProductNeedUpdate(csvItem, existingProduct);
          if (needsUpdate.hasChanges) {
            productsToUpdate.push({
              csvItem,
              existingProduct,
              changes: needsUpdate.changes
            });
            logger.debug(`Product needs update: ${csvItem.Item} - Changes: ${needsUpdate.changes.join(', ')}`);
          } else {
            logger.debug(`Product up to date: ${csvItem.Item}`);
          }
        } else {
          logger.debug(`Product exists, updates disabled: ${csvItem.Item}`);
        }
      } else {
        // Product doesn't exist, add to create batch
        productsToCreate.push({ csvItem });
        logger.debug(`New product to create: ${csvItem.Item}`);
      }
    });

    return { productsToCreate, productsToUpdate };
  }

  /**
   * Check if a product needs updates by comparing CSV data with existing product
   */
  doesProductNeedUpdate(csvItem, existingProduct) {
    const changes = [];
    let hasChanges = false;

    try {
      // Check title
      const csvTitle = csvItem.Description || csvItem.Item || 'Untitled Product';
      if (existingProduct.title !== csvTitle) {
        changes.push('title');
        hasChanges = true;
      }

      // Check price (from first variant)
      const csvPrice = this.parsePrice(csvItem.MSRP || csvItem.ContractPrice);
      const existingPrice = existingProduct.variants?.[0]?.price;
      if (existingPrice && parseFloat(existingPrice) !== parseFloat(csvPrice)) {
        changes.push('price');
        hasChanges = true;
      }

      // Check inventory
      const csvInventory = parseInt(csvItem.Qty_Avail) || 0;
      const existingInventory = existingProduct.variants?.[0]?.inventory_quantity;
      if (existingInventory !== undefined && existingInventory !== csvInventory) {
        changes.push('inventory');
        hasChanges = true;
      }

      // Check status
      const csvStatus = csvItem.Status === 'Active' ? 'active' : 'draft';
      if (existingProduct.status !== csvStatus) {
        changes.push('status');
        hasChanges = true;
      }

    } catch (error) {
      logger.warn(`Error checking updates for ${csvItem.Item}:`, error.message);
      // If we can't determine changes, assume no update needed
      return { hasChanges: false, changes: [] };
    }

    return { hasChanges, changes };
  }

  // Process batch items in parallel with create/update support
  async processBatchParallelWithUpdates(batch, results, dryRun) {
    const promises = batch.map(async (item) => {
      try {
        if (item.action === 'create') {
          return await this.processCreateItem(item.csvItem, dryRun);
        } else if (item.action === 'update') {
          return await this.processUpdateItem(item.csvItem, item.existingProduct, item.changes, dryRun);
        }
      } catch (error) {
        logger.error(`✗ Failed to process ${item.csvItem.Item || item.csvItem.Description}:`, error.message);
        return {
          success: false,
          error: error.message,
          csvItem: item.csvItem.Item || 'Unknown',
          title: item.csvItem.Description,
          action: item.action
        };
      }
    });

    // Execute all promises in parallel
    const batchResults = await Promise.allSettled(promises);

    // Process results
    let batchCreated = 0;
    let batchUpdated = 0;
    let batchErrors = 0;

    batchResults.forEach((result, index) => {
      if (result.status === 'fulfilled') {
        const value = result.value;
        if (value.success) {
          if (value.type === 'created') {
            results.created++;
            batchCreated++;
            results.createdProducts.push(value.product);
          } else if (value.type === 'updated') {
            results.updated++;
            batchUpdated++;
            results.updatedProducts.push(value.product);
          }
        } else {
          results.errors++;
          batchErrors++;
          results.errorDetails.push({
            csvItem: value.csvItem,
            error: value.error
          });
        }
      } else {
        results.errors++;
        batchErrors++;
        results.errorDetails.push({
          csvItem: batch[index]?.csvItem?.Item || 'Unknown',
          error: result.reason?.message || 'Promise rejected'
        });
      }
    });

    logger.info(`  Batch completed: ${batchCreated} created, ${batchUpdated} updated, ${batchErrors} errors`);
  }

  // Process batch items sequentially with create/update support
  async processBatchSequentialWithUpdates(batch, results, dryRun) {
    for (const item of batch) {
      try {
        let result;
        if (item.action === 'create') {
          result = await this.processCreateItem(item.csvItem, dryRun);
        } else if (item.action === 'update') {
          result = await this.processUpdateItem(item.csvItem, item.existingProduct, item.changes, dryRun);
        }

        if (result.success) {
          if (result.type === 'created') {
            results.created++;
            results.createdProducts.push(result.product);
          } else if (result.type === 'updated') {
            results.updated++;
            results.updatedProducts.push(result.product);
          }
        }

        // Small delay between individual products
        await this.delay(100);

      } catch (error) {
        results.errors++;
        const errorDetail = {
          csvItem: item.csvItem.Item || 'Unknown',
          error: error.message
        };
        results.errorDetails.push(errorDetail);
        logger.error(`✗ Failed to process ${item.csvItem.Item || item.csvItem.Description}:`, error.message);
      }
    }
  }

  // Process a single create item
  async processCreateItem(csvItem, dryRun) {
    const productData = this.mapCSVToShopifyProduct(csvItem);

    if (dryRun) {
      logger.debug(`[DRY RUN] Would create product: ${productData.title}`);
      return { success: true, type: 'dry-run-create', product: productData };
    } else {
      const createdProduct = await this.createProduct(productData);
      logger.debug(`✓ Created: ${createdProduct.title} (ID: ${createdProduct.id})`);
      return { success: true, type: 'created', product: createdProduct };
    }
  }

  // Process a single update item
  async processUpdateItem(csvItem, existingProduct, changes, dryRun) {
    if (dryRun) {
      logger.debug(`[DRY RUN] Would update product: ${existingProduct.title} - Changes: ${changes.join(', ')}`);
      return { success: true, type: 'dry-run-update', product: existingProduct };
    } else {
      try {
        const updateData = this.buildUpdateData(csvItem, existingProduct, changes);
        const updatedProduct = await this.updateProduct(existingProduct.id, updateData);
        logger.debug(`✓ Updated: ${updatedProduct.title} (ID: ${updatedProduct.id}) - Changes: ${changes.join(', ')}`);
        return { success: true, type: 'updated', product: updatedProduct };
      } catch (error) {
        logger.error(`✗ Failed to update product ${existingProduct.id}:`, error.message);
        throw error;
      }
    }
  }

  /**
   * Build update data based on changes needed
   */
  buildUpdateData(csvItem, existingProduct, changes) {
    const updateData = { id: existingProduct.id };

    if (changes.includes('title')) {
      updateData.title = csvItem.Description || csvItem.Item || 'Untitled Product';
    }

    if (changes.includes('status')) {
      updateData.status = csvItem.Status === 'Active' ? 'active' : 'draft';
    }

    // For price and inventory updates, we need to update variants
    if (changes.includes('price') || changes.includes('inventory')) {
      const variant = existingProduct.variants?.[0];
      if (variant) {
        updateData.variants = [{
          id: variant.id,
          ...(changes.includes('price') && { 
            price: this.parsePrice(csvItem.MSRP || csvItem.ContractPrice) 
          }),
          ...(changes.includes('inventory') && { 
            inventory_quantity: parseInt(csvItem.Qty_Avail) || 0 
          })
        }];
      }
    }

    return updateData;
  }

  /**
   * Update an existing product in Shopify
   */
  async updateProduct(productId, updateData) {
    if (!this.initialized) {
      await this.initialize();
    }

    try {
      const response = await axios.put(`${this.baseURL}/products/${productId}.json`, {
        product: updateData
      }, { headers: this.headers });

      const product = response.data.product;
      logger.debug(`Updated product: ${product.title} (ID: ${product.id})`);
      return product;

    } catch (error) {
      logger.error('Error updating product in Shopify:', error.message);
      if (error.response && error.response.data) {
        logger.error('Shopify API Error Details:', JSON.stringify(error.response.data, null, 2));
      }
      throw error;
    }
  }

  // Legacy method for backward compatibility - now calls the new processProductsFromCSV
  async createProductsFromCSV(csvData, shopifyProducts, options = {}) {
    // Add legacy options mapping
    const newOptions = {
      ...options,
      enableUpdates: false // Legacy mode: only create, don't update
    };

    logger.warn('Using legacy createProductsFromCSV method. Consider switching to processProductsFromCSV for create+update functionality.');
    return await this.processProductsFromCSV(csvData, shopifyProducts, newOptions);
  }

  // Process batch items in parallel using Promise.all
  async processBatchParallel(batch, results, dryRun) {
    const promises = batch.map(async (csvItem) => {
      try {
        // Convert CSV item to Shopify product format
        const productData = this.mapCSVToShopifyProduct(csvItem);

        if (dryRun) {
          logger.debug(`[DRY RUN] Would create product: ${productData.title}`);
          return { success: true, type: 'dry-run', product: productData };
        } else {
          const createdProduct = await this.createProduct(productData);
          logger.debug(`✓ Created: ${createdProduct.title} (ID: ${createdProduct.id})`);
          return { success: true, type: 'created', product: createdProduct };
        }
      } catch (error) {
        logger.error(`✗ Failed to create product ${csvItem.Item || csvItem.Description}:`, error.message);
        return {
          success: false,
          error: error.message,
          csvItem: csvItem.Item || 'Unknown',
          title: csvItem.Description
        };
      }
    });

    // Execute all promises in parallel
    const batchResults = await Promise.allSettled(promises);

    // Process results
    let batchCreated = 0;
    let batchErrors = 0;

    batchResults.forEach((result, index) => {
      if (result.status === 'fulfilled') {
        const value = result.value;
        if (value.success) {
          results.created++;
          batchCreated++;
          if (value.type === 'created') {
            results.createdProducts.push(value.product);
          }
        } else {
          results.errors++;
          batchErrors++;
          results.errorDetails.push({
            csvItem: value.csvItem,
            error: value.error
          });
        }
      } else {
        results.errors++;
        batchErrors++;
        results.errorDetails.push({
          csvItem: batch[index]?.Item || 'Unknown',
          error: result.reason?.message || 'Promise rejected'
        });
      }
    });

    logger.info(`  Batch completed: ${batchCreated} created, ${batchErrors} errors`);
  }

  // Process batch items sequentially (original method)
  async processBatchSequential(batch, results, dryRun) {
    for (const csvItem of batch) {
      try {
        // Convert CSV item to Shopify product format
        const productData = this.mapCSVToShopifyProduct(csvItem);

        if (dryRun) {
          logger.debug(`[DRY RUN] Would create product: ${productData.title}`);
          results.created++;
        } else {
          const createdProduct = await this.createProduct(productData);
          results.created++;
          results.createdProducts.push(createdProduct);
          logger.debug(`✓ Created: ${createdProduct.title} (ID: ${createdProduct.id})`);
        }

        // Small delay between individual products
        await this.delay(100);

      } catch (error) {
        results.errors++;
        const errorDetail = {
          csvItem: csvItem.Item || 'Unknown',
          error: error.message
        };
        results.errorDetails.push(errorDetail);
        logger.error(`✗ Failed to create product ${csvItem.Item || csvItem.Description}:`, error.message);
      }
    }
  }

  // Map CSV data to Shopify product format
  mapCSVToShopifyProduct(csvItem) {
    const product = {
      title: csvItem.Description || csvItem.Item || 'Untitled Product',
      handle: this.generateHandle(csvItem.Description || csvItem.Item),
      body_html: this.generateProductDescription(csvItem),
      vendor: 'QGold',
      product_type: this.extractProductType(csvItem),
      // status: 'draft',
      // TODO: now we only want them in draft mode
      status: csvItem.Status === 'Active' ? 'active' : 'draft',
      published: csvItem.Status === 'Active',
      // published: false,
      tags: this.generateTags(csvItem),
      variants: [this.createVariant(csvItem)],
      images: this.createImages(csvItem),
      metafields: this.createMetafields(csvItem),
      category: this.getCategoryId(csvItem)
    };

    // Add SEO fields
    if (csvItem.Description) {
      product.seo_title = csvItem.Description.substring(0, 70);
      product.seo_description = this.generateSEODescription(csvItem);
    }

    return product;
  }

  // Map CSV categories to Shopify category IDs
  getCategoryId(csvItem) {
    if (!csvItem.Categories) {
      logger.debug('No categories found in CSV item, using default jewelry category');
      return this.defaultJewelryCategory;
    }

    // Split categories by semicolon and process each one
    const categories = csvItem.Categories.split(';');
    logger.debug(`Processing categories for ${csvItem.Item}: ${categories.join(' | ')}`);

    for (const categoryPath of categories) {
      const categoryId = this.findBestCategoryMatch(categoryPath);
      if (categoryId) {
        logger.info(`Category mapped for ${csvItem.Item}: ${categoryPath.trim()} -> ${categoryId}`);
        return categoryId;
      }
    }

    logger.warn(`No category match found for ${csvItem.Item}, using default jewelry category`);
    return this.defaultJewelryCategory;
  }

  // Find the best category match from a category path
  findBestCategoryMatch(categoryPath) {
    if (!categoryPath) return null;

    // Clean up the category path and convert to lowercase
    const cleanPath = categoryPath.trim().toLowerCase().replace(/\\/g, '');

    // Split by common separators and get individual category terms
    const categoryTerms = cleanPath.split(/[\\\/\>\|]/).map(term => term.trim()).filter(term => term);

    logger.debug(`Analyzing category terms: ${categoryTerms.join(', ')}`);

    // Check for specific matches in order of preference (most specific first)
    for (const term of categoryTerms.reverse()) { // Start from most specific (end of path)
      // Direct match
      if (this.categoryMap.has(term)) {
        return this.categoryMap.get(term);
      }

      // Check if term contains any of our category keywords
      for (const [keyword, categoryId] of this.categoryMap.entries()) {
        if (term.includes(keyword) || keyword.includes(term)) {
          return categoryId;
        }
      }
    }

    // If no match found, try looking for partial matches in the full path
    for (const [keyword, categoryId] of this.categoryMap.entries()) {
      if (cleanPath.includes(keyword)) {
        return categoryId;
      }
    }

    return null;
  }

  // Generate URL handle from title
  generateHandle(title) {
    if (!title) return 'product';
    return title
      .toLowerCase()
      .replace(/[^a-z0-9\s-]/g, '') // Remove special characters
      .replace(/\s+/g, '-') // Replace spaces with hyphens
      .replace(/-+/g, '-') // Remove multiple consecutive hyphens
      .replace(/^-|-$/g, '') // Remove leading/trailing hyphens
      .substring(0, 100); // Limit length
  }

  // Create variant from CSV data
  createVariant(csvItem) {
    const variant = {
      title: 'Default Title',
      sku: csvItem.Item || '',
      barcode: csvItem.UPC || '',
      price: this.parsePrice(csvItem.MSRP || csvItem.ContractPrice),
      compare_at_price: csvItem.MSRP ? this.parsePrice(csvItem.MSRP) : null,
      inventory_management: 'shopify',
      inventory_quantity: parseInt(csvItem.Qty_Avail) || 0,
      weight: this.parseWeight(csvItem.Weight),
      weight_unit: 'g'
    };

    // Add variant options if available
    if (csvItem.Size || csvItem.Length || csvItem.Width) {
      variant.option1 = csvItem.Size || `${csvItem.Length || ''}x${csvItem.Width || ''}`.replace('x', '');
    }

    return variant;
  }

  // Create images array from CSV data
  createImages(csvItem) {
    const images = [];

    // Primary image
    if (csvItem.ImageLink_1000) {
      images.push({
        src: csvItem.ImageLink_1000,
        alt: csvItem.Description || csvItem.Item
      });
    }

    // Additional images
    for (let i = 1; i <= 9; i++) {
      const imageLink = csvItem[`Image${i}Link`];
      if (imageLink && imageLink.trim()) {
        images.push({
          src: imageLink,
          alt: `${csvItem.Description || csvItem.Item} - View ${i}`
        });
      }
    }

    return images;
  }

  // Create metafields for additional data
  createMetafields(csvItem) {
    const metafields = [];

    // Store original item number
    if (csvItem.Item) {
      metafields.push({
        namespace: 'custom',
        key: 'item_number',
        value: csvItem.Item,
        type: 'single_line_text_field'
      });
    }

    // Store specifications
    if (csvItem.ListOfSpecs) {
      metafields.push({
        namespace: 'custom',
        key: 'specifications',
        value: csvItem.ListOfSpecs,
        type: 'multi_line_text_field'
      });
    }

    // Store metal description
    if (csvItem.Metal_Desc) {
      metafields.push({
        namespace: 'custom',
        key: 'metal_description',
        value: csvItem.Metal_Desc,
        type: 'single_line_text_field'
      });
    }

    // Store country of origin
    if (csvItem.Country_Of_Origin) {
      metafields.push({
        namespace: 'custom',
        key: 'country_of_origin',
        value: csvItem.Country_Of_Origin,
        type: 'single_line_text_field'
      });
    }

    return metafields;
  }

  // Generate product description from CSV data
  generateProductDescription(csvItem) {
    let description = '';

    if (csvItem.Description) {
      description += `<h3>${csvItem.Description}</h3>`;
    }

    if (csvItem.Metal_Desc) {
      description += `<p><strong>Material:</strong> ${csvItem.Metal_Desc}</p>`;
    }

    if (csvItem.Weight) {
      description += `<p><strong>Weight:</strong> ${csvItem.Weight}g</p>`;
    }

    if (csvItem.Length && csvItem.Width) {
      description += `<p><strong>Dimensions:</strong> ${csvItem.Length}" x ${csvItem.Width}"</p>`;
    } else if (csvItem.Length) {
      description += `<p><strong>Length:</strong> ${csvItem.Length}"</p>`;
    }

    if (csvItem.ListOfSpecs) {
      description += `<div><strong>Specifications:</strong><br>`;
      const specs = csvItem.ListOfSpecs.split('|');
      specs.forEach(spec => {
        if (spec.trim()) {
          description += `• ${spec.trim()}<br>`;
        }
      });
      description += '</div>';
    }

    return description || '<p>Quality jewelry piece</p>';
  }

  // Generate SEO description
  generateSEODescription(csvItem) {
    const desc = csvItem.Description || '';
    const metal = csvItem.Metal_Desc || '';
    return `${desc} ${metal}`.trim().substring(0, 160);
  }

  // Extract product type from categories
  extractProductType(csvItem) {
    if (csvItem.Categories) {
      const categories = csvItem.Categories.split(';')[0]; // Take first category
      const cleanCategories = categories.toLowerCase();

      // More comprehensive product type detection (order matters - check more specific terms first)
      if (cleanCategories.includes('necklaces') || cleanCategories.includes('chains')) return 'Necklaces';
      if (cleanCategories.includes('earrings')) return 'Earrings';
      if (cleanCategories.includes('bracelets')) return 'Bracelets';
      if (cleanCategories.includes('anklets')) return 'Anklets';
      if (cleanCategories.includes('rings')) return 'Rings';
      if (cleanCategories.includes('pendants') || cleanCategories.includes('charms')) return 'Pendants & Charms';
      if (cleanCategories.includes('watch accessories') || cleanCategories.includes('watch bands')) return 'Watch Accessories';
      if (cleanCategories.includes('watches')) return 'Watches';
      if (cleanCategories.includes('brooches') || cleanCategories.includes('lapel pins')) return 'Brooches & Pins';
      if (cleanCategories.includes('jewelry sets') || cleanCategories.includes('sets')) return 'Jewelry Sets';
      if (cleanCategories.includes('body jewelry')) return 'Body Jewelry';
    }
    return 'Jewelry';
  }

  // Generate tags from CSV data
  generateTags(csvItem) {
    const tags = [];

    if (csvItem.Metal_Desc) {
      tags.push(csvItem.Metal_Desc);
    }

    if (csvItem.Attributes) {
      const attributes = csvItem.Attributes.split(';');
      tags.push(...attributes.filter(attr => attr.trim()));
    }

    if (csvItem.ProductLine) {
      tags.push(csvItem.ProductLine);
    }

    return tags.join(', ');
  }

  // Parse price from string
  parsePrice(priceStr) {
    if (!priceStr) return '0.00';
    const price = parseFloat(priceStr.toString().replace(/[^\d.-]/g, ''));
    return isNaN(price) ? '0.00' : price.toFixed(2);
  }

  // Parse weight from string
  parseWeight(weightStr) {
    if (!weightStr) return 0;
    const weight = parseFloat(weightStr.toString().replace(/[^\d.-]/g, ''));
    return isNaN(weight) ? 0 : Math.round(weight * 28.35); // Convert oz to grams
  }

  // Fast duplicate detection using lookup sets
  productExistsFast(csvItem, existingSkuSet, existingTitleSet) {
    const sku = csvItem.Item;
    const title = csvItem.Description;

    // Check by SKU (fastest lookup)
    if (sku && existingSkuSet.has(sku.toLowerCase())) {
      return true;
    }

    // Check by title
    if (title && existingTitleSet.has(title.toLowerCase())) {
      return true;
    }

    return false;
  }

  // Original method kept for backward compatibility
  productExists(csvItem, existingProducts) {
    const sku = csvItem.Item;
    const title = csvItem.Description;

    return existingProducts.some(product => {
      // Check by SKU in variants
      const skuMatch = product.variants && product.variants.some(variant =>
        variant.sku && variant.sku.toLowerCase() === sku?.toLowerCase()
      );
      // Check by title similarity
      const titleMatch = product.title.toLowerCase() === title?.toLowerCase();

      return skuMatch || titleMatch;
    });
  }

  // Log product summary for debugging
  logProductSummary(products) {
    if (products.length === 0) {
      logger.info('No products found');
      return;
    }

    logger.info(`=== SHOPIFY PRODUCTS SUMMARY ===`);
    logger.info(`Total Products: ${products.length}`);

    // Log first few products as examples
    const sampleSize = Math.min(3, products.length);
    logger.info(`\nFirst ${sampleSize} products:`);

    for (let i = 0; i < sampleSize; i++) {
      const product = products[i];
      logger.info(`${i + 1}. ${product.title} (ID: ${product.id})`);
      logger.info(`   Handle: ${product.handle}`);
      logger.info(`   Status: ${product.status}`);
      logger.info(`   Variants: ${product.variants ? product.variants.length : 0}`);
      logger.info(`   Created: ${product.created_at}`);
      logger.info('   ---');
    }

    // Log product status distribution
    const statusCount = products.reduce((acc, product) => {
      acc[product.status] = (acc[product.status] || 0) + 1;
      return acc;
    }, {});

    logger.info('\nProduct Status Distribution:');
    Object.entries(statusCount).forEach(([status, count]) => {
      logger.info(`  ${status}: ${count} products`);
    });

    logger.info(`=== END SUMMARY ===\n`);
  }
}

module.exports = ShopifyClient;
