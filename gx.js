const ShopifyClient = require('./src/shopifyClient');
const logger = require('./src/logger');

class ProductDeleter {
  constructor() {
    this.shopifyClient = new ShopifyClient();
  }

  /**
   * Delete products created on a specific date
   * @param {string} targetDate - Date in YYYY-MM-DD format
   * @param {object} options - Configuration options
   */
  async deleteProductsByDate(targetDate, options = {}) {
    const {
      dryRun = false,
      batchSize = 10,
      delayBetweenBatches = 1000,
      confirmBeforeDelete = true
    } = options;

    logger.info(`=== PRODUCT DELETION SCRIPT ===`);
    logger.info(`Target date: ${targetDate}`);
    logger.info(`Dry run: ${dryRun}`);
    logger.info(`Batch size: ${batchSize}`);
    logger.info(`Delay between batches: ${delayBetweenBatches}ms`);
    logger.info('='.repeat(50));

    try {
      // Initialize Shopify client
      await this.shopifyClient.initialize();
      logger.info('✓ Shopify client initialized');

      // Fetch all products
      logger.info('Fetching all products from Shopify...');
      const allProducts = await this.shopifyClient.getAllProducts();
      logger.info(`✓ Fetched ${allProducts.length} total products`);

      // Filter products by creation date
      const targetProducts = this.filterProductsByDate(allProducts, targetDate);
      logger.info(`✓ Found ${targetProducts.length} products created on ${targetDate}`);

      if (targetProducts.length === 0) {
        logger.info('No products found for the specified date. Exiting.');
        return {
          total: 0,
          deleted: 0,
          errors: 0,
          skipped: 0
        };
      }

      // Log sample products for review
      this.logProductSample(targetProducts);

      // Confirm deletion if required
      if (confirmBeforeDelete && !dryRun) {
        const confirmed = await this.confirmDeletion(targetProducts.length);
        if (!confirmed) {
          logger.info('Deletion cancelled by user.');
          return {
            total: targetProducts.length,
            deleted: 0,
            errors: 0,
            skipped: targetProducts.length
          };
        }
      }

      // Process deletions in batches
      const results = await this.processProductDeletions(targetProducts, {
        dryRun,
        batchSize,
        delayBetweenBatches
      });

      // Log final results
      this.logFinalResults(results);

      return results;

    } catch (error) {
      logger.error('Error in deleteProductsByDate:', error.message);
      throw error;
    }
  }

  /**
   * Filter products by creation date
   * @param {Array} products - Array of Shopify products
   * @param {string} targetDate - Target date in YYYY-MM-DD format
   */
  filterProductsByDate(products, targetDate) {
    return products
    return products.filter(product => {
      if (!product.created_at) return false;

      // Extract date part from created_at timestamp
      // Example: "2025-10-14T21:04:49-04:00" -> "2025-10-14"
      const createdDate = product.created_at.split('T')[0];
      return createdDate === targetDate;
    });
  }

  /**
   * Log a sample of products that will be deleted
   */
  logProductSample(products) {
    const sampleSize = Math.min(5, products.length);
    logger.info(`\nSample of products to be deleted (showing ${sampleSize} of ${products.length}):`);
    
    for (let i = 0; i < sampleSize; i++) {
      const product = products[i];
      logger.info(`${i + 1}. ${product.title} (ID: ${product.id})`);
      logger.info(`   Created: ${product.created_at}`);
      logger.info(`   Status: ${product.status}`);
      logger.info(`   Handle: ${product.handle}`);
      logger.info('   ---');
    }
    
    if (products.length > sampleSize) {
      logger.info(`... and ${products.length - sampleSize} more products`);
    }
  }

  /**
   * Confirm deletion with user (for interactive mode)
   */
  async confirmDeletion(productCount) {
    // For now, return true to proceed automatically
    // In a real interactive environment, you might want to add readline prompts
    logger.warn(`⚠️  About to delete ${productCount} products. This action cannot be undone!`);
    logger.info('Proceeding with deletion in 3 seconds...');
    
    // Add a small delay to allow user to cancel if needed
    await this.delay(3000);
    return true;
  }

  /**
   * Process product deletions in batches
   */
  async processProductDeletions(products, options) {
    const { dryRun, batchSize, delayBetweenBatches } = options;

    const results = {
      total: products.length,
      deleted: 0,
      errors: 0,
      skipped: 0,
      errorDetails: []
    };

    // Process in batches
    for (let i = 0; i < products.length; i += batchSize) {
      const batch = products.slice(i, i + batchSize);
      const batchNumber = Math.floor(i / batchSize) + 1;
      const totalBatches = Math.ceil(products.length / batchSize);

      logger.info(`Processing batch ${batchNumber}/${totalBatches} (${batch.length} products)`);

      try {
        await this.processBatchDeletion(batch, results, dryRun);
      } catch (error) {
        logger.error(`Batch ${batchNumber} failed:`, error.message);
        results.errors += batch.length;
        batch.forEach(product => {
          results.errorDetails.push({
            productId: product.id,
            title: product.title,
            error: `Batch failure: ${error.message}`
          });
        });
      }

      // Delay between batches (except for the last batch)
      if (i + batchSize < products.length) {
        logger.info(`Waiting ${delayBetweenBatches}ms before next batch...`);
        await this.delay(delayBetweenBatches);
      }
    }

    return results;
  }

  /**
   * Process a single batch of product deletions
   */
  async processBatchDeletion(batch, results, dryRun) {
    const promises = batch.map(async (product) => {
      try {
        if (dryRun) {
          logger.debug(`[DRY RUN] Would delete: ${product.title} (ID: ${product.id})`);
          return { success: true, type: 'dry-run', product };
        } else {
          await this.deleteProduct(product.id);
          logger.debug(`✓ Deleted: ${product.title} (ID: ${product.id})`);
          return { success: true, type: 'deleted', product };
        }
      } catch (error) {
        logger.error(`✗ Failed to delete product ${product.id} (${product.title}):`, error.message);
        return {
          success: false,
          error: error.message,
          product
        };
      }
    });

    // Execute all promises in parallel
    const batchResults = await Promise.allSettled(promises);

    // Process results
    let batchDeleted = 0;
    let batchErrors = 0;

    batchResults.forEach((result, index) => {
      if (result.status === 'fulfilled') {
        const value = result.value;
        if (value.success) {
          results.deleted++;
          batchDeleted++;
        } else {
          results.errors++;
          batchErrors++;
          results.errorDetails.push({
            productId: value.product.id,
            title: value.product.title,
            error: value.error
          });
        }
      } else {
        results.errors++;
        batchErrors++;
        results.errorDetails.push({
          productId: batch[index]?.id || 'Unknown',
          title: batch[index]?.title || 'Unknown',
          error: result.reason?.message || 'Promise rejected'
        });
      }
    });

    logger.info(`  Batch completed: ${batchDeleted} deleted, ${batchErrors} errors`);
  }

  /**
   * Delete a single product by ID
   */
  async deleteProduct(productId) {
    try {
      const response = await require('axios').delete(
        `${this.shopifyClient.baseURL}/products/${productId}.json`,
        { headers: this.shopifyClient.headers }
      );

      if (response.status === 200) {
        return true;
      } else {
        throw new Error(`Unexpected response status: ${response.status}`);
      }
    } catch (error) {
      if (error.response) {
        const status = error.response.status;
        const data = error.response.data;
        
        if (status === 404) {
          throw new Error('Product not found (may have been deleted already)');
        } else if (status === 422) {
          throw new Error(`Unprocessable entity: ${JSON.stringify(data.errors || data)}`);
        } else if (status === 429) {
          throw new Error('Rate limited - too many requests');
        } else {
          throw new Error(`API Error ${status}: ${JSON.stringify(data || error.response.statusText)}`);
        }
      }
      throw error;
    }
  }

  /**
   * Log final results
   */
  logFinalResults(results) {
    logger.info('='.repeat(50));
    logger.info(`=== DELETION COMPLETE ===`);
    logger.info(`Total products processed: ${results.total}`);
    logger.info(`Successfully deleted: ${results.deleted}`);
    logger.info(`Errors: ${results.errors}`);
    logger.info(`Skipped: ${results.skipped}`);

    if (results.errors > 0) {
      logger.info('\nError details:');
      results.errorDetails.slice(0, 10).forEach((detail, index) => {
        logger.error(`${index + 1}. Product ${detail.productId} (${detail.title}): ${detail.error}`);
      });
      if (results.errorDetails.length > 10) {
        logger.info(`... and ${results.errorDetails.length - 10} more errors`);
      }
    }

    const successRate = results.total > 0 ? ((results.deleted / results.total) * 100).toFixed(1) : 0;
    logger.info(`Success rate: ${successRate}%`);
    logger.info('='.repeat(50));
  }

  /**
   * Helper method to add delay
   */
  delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

// Main execution function
async function main() {
  const deleter = new ProductDeleter();

  try {
    // Configuration
    const targetDate = '2025-10-16'; // October 14th, 2025
    const options = {
      dryRun: false, // Set to true for testing without actual deletion
      batchSize: 5, // Smaller batches for deletion to be safer
      delayBetweenBatches: 2000, // 2 seconds between batches
      confirmBeforeDelete: true
    };

    // Process command line arguments
    const args = process.argv.slice(2);
    if (args.includes('--dry-run')) {
      options.dryRun = true;
      logger.info('Running in DRY RUN mode - no products will be deleted');
    }

    if (args.includes('--date')) {
      const dateIndex = args.indexOf('--date');
      if (args[dateIndex + 1]) {
        targetDate = args[dateIndex + 1];
        logger.info(`Using custom date: ${targetDate}`);
      }
    }

    // Execute deletion
    const results = await deleter.deleteProductsByDate(targetDate, options);

    // Exit with appropriate code
    if (results.errors > 0) {
      process.exit(1);
    } else {
      process.exit(0);
    }

  } catch (error) {
    logger.error('Script failed:', error.message);
    process.exit(1);
  }
}

// Run if this file is executed directly
// if (require.main === module) {
//   main();
// }
main()

module.exports = ProductDeleter;