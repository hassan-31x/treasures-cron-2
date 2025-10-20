#!/usr/bin/env node

/**
 * Debug script to test basic functionality and environment setup
 */

const ProductProcessor = require('./src/index');
const ShopifyClient = require('./src/shopifyClient');
const logger = require('./src/logger');

async function debugTest() {
  try {
    logger.info('=== DEBUGGING APPLICATION SETUP ===');
    
    // Test 1: Environment variables
    logger.info('1. Checking environment variables...');
    if (!process.env.SHOPIFY_STORE_URL || !process.env.SHOPIFY_ACCESS_TOKEN) {
      logger.error('❌ Missing environment variables. Please check .env file');
      logger.info('Required: SHOPIFY_STORE_URL, SHOPIFY_ACCESS_TOKEN');
      return;
    }
    logger.info('✓ Environment variables found');

    // Test 2: Shopify connection
    logger.info('2. Testing Shopify connection...');
    const client = new ShopifyClient();
    await client.initialize();
    logger.info('✓ Shopify connection successful');

    // Test 3: CSV processing
    logger.info('3. Testing CSV processing...');
    const processor = new ProductProcessor();
    
    // Quick file check
    const fs = require('fs');
    const csvPath = processor.csvFilePath;
    if (!fs.existsSync(csvPath)) {
      logger.error(`❌ CSV file not found: ${csvPath}`);
      return;
    }
    logger.info(`✓ CSV file found: ${csvPath}`);

    // Test 4: Filter logic
    logger.info('4. Testing filter logic...');
    const sampleData = [
      {
        Item: 'TEST-001',
        Description: 'Diamond Engagement Ring',
        MSRP: '1500.00',
        ProductLine: 'engagement rings',
        Status: 'Active'
      }
    ];
    
    const filtered = processor.filterProducts(sampleData);
    if (filtered.length === 1) {
      logger.info('✓ Filter logic working correctly');
    } else {
      logger.error('❌ Filter logic failed');
      return;
    }

    // Test 5: Product mapping
    logger.info('5. Testing product mapping...');
    const shopifyProduct = client.mapCSVToShopifyProduct(sampleData[0]);
    if (shopifyProduct && shopifyProduct.title && shopifyProduct.variants) {
      logger.info('✓ Product mapping successful');
      logger.info(`Mapped product: ${shopifyProduct.title}`);
      logger.info(`Category: ${shopifyProduct.category}`);
    } else {
      logger.error('❌ Product mapping failed');
      return;
    }

    logger.info('\n=== ALL TESTS PASSED ===');
    logger.info('The application setup is working correctly!');
    logger.info('\nNext steps:');
    logger.info('- Run "npm test" for dry run testing');
    logger.info('- Run "npm run test-single" to test creating one product');
    logger.info('- Run "npm run preview" to see filtered products');
    logger.info('- Run "npm start" to begin full batch processing');
    
  } catch (error) {
    logger.error('❌ Debug test failed:', error.message);
    logger.error('Stack trace:', error.stack);
    process.exit(1);
  }
}

if (require.main === module) {
  debugTest();
}

module.exports = debugTest;