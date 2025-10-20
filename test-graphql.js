#!/usr/bin/env node

/**
 * Test script to verify Shopify GraphQL connection and category mapping
 */

const ShopifyClient = require('./src/shopifyClient');
const logger = require('./src/logger');

async function testGraphQL() {
  try {
    logger.info('=== Testing Shopify GraphQL Connection ===');
    
    const client = new ShopifyClient();
    
    // Initialize and test connection
    await client.initialize();
    
    logger.info('✓ Connection test passed');
    
    // Test category mapping for the sample product
    const sampleCsvItem = {
      Item: 'TEST-123',
      Description: '14K Rose Gold Chain Necklace',
      Categories: '\\Jewelry\\Necklaces\\Chain Necklaces\\Rope Chain Necklaces;\\Jewelry\\Chains\\Rope Chains\\Rope Chain'
    };
    
    const categoryId = client.getCategoryId(sampleCsvItem);
    logger.info(`✓ Category mapping test: ${categoryId}`);
    
    logger.info('=== All tests passed! ===');
    
  } catch (error) {
    logger.error('❌ Test failed:', error.message);
    process.exit(1);
  }
}

if (require.main === module) {
  testGraphQL();
}

module.exports = testGraphQL;