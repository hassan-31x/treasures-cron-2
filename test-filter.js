#!/usr/bin/env node

/**
 * Quick test to verify the filtering logic with sample data
 */

const ProductProcessor = require('./src/index');
const logger = require('./src/logger');

async function testFiltering() {
  try {
    logger.info('=== Testing Product Filtering Logic ===');
    
    const processor = new ProductProcessor();
    
    // Test with sample data that matches our CSV structure
    const sampleData = [
      {
        Item: 'TEST-001',
        Description: 'Diamond Engagement Ring',
        MSRP: '1500.00',
        ProductLine: 'engagement rings',
        Status: 'Active'
      },
      {
        Item: 'TEST-002', 
        Description: 'Gold Chain Necklace',
        MSRP: '800.00', // Below minimum
        ProductLine: 'chains',
        Status: 'Active'
      },
      {
        Item: 'TEST-003',
        Description: 'Bridal Set Ring',
        MSRP: '1800.00',
        ProductLine: 'Bridal',
        Status: 'Active'
      },
      {
        Item: 'TEST-004',
        Description: 'Random Product',
        MSRP: '1200.00',
        ProductLine: 'Other Category', // Doesn't match
        Status: 'Active'
      }
    ];

    logger.info(`Testing with ${sampleData.length} sample products:`);
    sampleData.forEach((p, i) => {
      logger.info(`${i+1}. ${p.Description} - MSRP: $${p.MSRP}, Line: ${p.ProductLine}`);
    });

    const filtered = processor.filterProducts(sampleData);
    
    logger.info(`\nFiltered results: ${filtered.length} products match criteria`);
    filtered.forEach((p, i) => {
      logger.info(`${i+1}. ✓ ${p.Description} - MSRP: $${p.MSRP}, Line: ${p.ProductLine}`);
    });
    
    logger.info('\n=== Filter Test Complete ===');
    logger.info('Expected: 2 products should match (TEST-001 and TEST-003)');
    logger.info(`Actual: ${filtered.length} products matched`);
    
    if (filtered.length === 2) {
      logger.info('✅ Filter test PASSED');
    } else {
      logger.error('❌ Filter test FAILED');
    }
    
  } catch (error) {
    logger.error('❌ Test failed:', error.message);
    process.exit(1);
  }
}

if (require.main === module) {
  testFiltering();
}

module.exports = testFiltering;