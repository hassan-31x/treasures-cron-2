require('dotenv').config();
const path = require('path');
const CSVProcessor = require('./csvProcessor');
const ShopifyClient = require('./shopifyClient');
const logger = require('./logger');

/**
 * Main application that processes CSV data and creates products in Shopify
 * Currently configured to process only the first product from partial.csv
 */
class ProductProcessor {
    constructor() {
        this.csvProcessor = new CSVProcessor();
        this.shopifyClient = new ShopifyClient();
        // Update to use the full products.csv file
        this.csvFilePath = path.join(__dirname, '..', 'data', 'products.csv');
        
        // Product filtering criteria
        this.filterCriteria = {
            msrpMin: 100,
            msrpMax: 1000,
            productLines: [
                'Breast Cancer Awareness',
                'Colorful Collections',
                'Dainty Designs',
                'Diamond Fascination',
                'Earring Jackets',
                'Fancy Diamond Hoops',
                'Fine Diamond Jewelry',
                'IBGoodman',
                'Infinity',
                'Inverness',
                'Italian Gold',
                'Lab Grown Diamond Jewelry',
                'Lockets',
                'Madi K',
                'Pearls',
                'Premier',
                'Sea Inspired Silver Jewelry',
                'Sideways Crosses',
                'Simply Starz',
                'South Sea and Tahitian Pearls',
                'Stackable Expressions',
                'Two Stone Collection',
                'Vibrant',
                'Wedding Bands USA'
            ]
        };
        
        // Batch processing configuration
        this.batchConfig = {
            batchSize: 10,
            maxConcurrentBatches: 3,
            delayBetweenBatches: 2000 // 2 seconds
        };
    }

    /**
     * Filter products based on MSRP, Product Line criteria, and exclusion rules
     */
    filterProducts(products) {
        return products.filter(product => {
            // 1. Exclude discontinued products
            const status = (product.Status || '').toLowerCase().trim();
            if (status === 'discontinued') {
                return false;
            }

            // 2. Check MSRP criteria
            const msrp = parseFloat(product.ContractPrice);
            if (isNaN(msrp) || msrp < this.filterCriteria.msrpMin || msrp > this.filterCriteria.msrpMax) {
                return false;
            }

            // 3. Check Product Line criteria
            const productLine = (product.ProductLine || '').toLowerCase().trim();
            const hasMatchingProductLine = this.filterCriteria.productLines.some(line => {
                const lineLower = line.toLowerCase();
                // Exact match or contains the keyword
                return productLine === lineLower || productLine.includes(lineLower);
            });

            if (!hasMatchingProductLine) {
                return false;
            }

            // 4. Exclusion criteria - exclude products that match any of these
            
            // Exclude if Categories contain "Finding" or "Mounting"
            const categories = (product.Categories || '').toLowerCase();
            if (categories.includes('finding') || categories.includes('mounting')) {
                return false;
            }

            // Exclude if Item_Type contains "Finding" or "Mounting"
            const itemType = (product.Item_Type || '').toLowerCase();
            if (itemType.includes('finding') || itemType.includes('mounting')) {
                return false;
            }

            // Exclude if title/description contains "Engraveable"
            const title = (product.Description || '').toLowerCase();
            if (title.includes('engraveable')) {
                return false;
            }

            // Exclude if Product Line contains "Personalized"
            if (productLine.includes('personalized')) {
                return false;
            }

            // Exclude if Categories contain "Personalized"
            if (categories.includes('personalized')) {
                return false;
            }

            // 5. Additional quality checks
            if (!product.Item || !product.Description) {
                return false;
            }

            return true;
        });
    }

    /**
     * Process products in streaming fashion with filtering and batching
     */
    async processFilteredProductsBatch() {
        try {
            logger.info('=== STARTING BATCH PROCESSING OF FILTERED PRODUCTS ===');
            logger.info(`Reading from: ${this.csvFilePath}`);
            logger.info(`Filter criteria:`, JSON.stringify(this.filterCriteria, null, 2));
            logger.info(`Batch configuration:`, JSON.stringify(this.batchConfig, null, 2));

            // Step 1: Initialize Shopify client
            logger.info('Step 1: Initializing Shopify client...');
            await this.shopifyClient.initialize();
            logger.info('✓ Shopify client initialized successfully');

            // Step 2: Get CSV file information
            logger.info('Step 2: Getting CSV file information...');
            const fileInfo = await this.csvProcessor.getFileInfo(this.csvFilePath);
            logger.info(`✓ CSV file loaded: ${fileInfo.sizeMB}MB, estimated ${fileInfo.estimatedRows} rows`);

            // Step 3: Process CSV file and apply filters
            logger.info('Step 3: Processing CSV file and applying filters...');
            const csvResult = await this.csvProcessor.processFile(this.csvFilePath);
            logger.info(`✓ CSV processed: ${csvResult.stats.processedRows} total products loaded`);

            // Step 4: Apply filters
            logger.info('Step 4: Applying product filters...');
            const filteredProducts = this.filterProducts(csvResult.data);
            logger.info(`✓ Filtered products: ${filteredProducts.length} products match criteria`);

            if (filteredProducts.length === 0) {
                logger.warn('No products match the filter criteria');
                return { success: true, processed: 0, message: 'No matching products found' };
            }

            // Step 5: Process in batches
            logger.info('Step 5: Processing products in batches...');
            const batchResults = await this.processBatchedProducts(filteredProducts);

            logger.info('=== BATCH PROCESSING COMPLETED ===');
            logger.info(`Total processed: ${batchResults.totalProcessed}`);
            logger.info(`Successfully created: ${batchResults.created}`);
            logger.info(`Errors: ${batchResults.errors}`);

            return batchResults;

        } catch (error) {
            logger.error('=== BATCH PROCESSING FAILED ===');
            logger.error('Error:', error.message);
            throw error;
        }
    }

    /**
     * Process products in batches with concurrent processing
     */
    async processBatchedProducts(products) {
        const results = {
            totalProcessed: 0,
            created: 0,
            errors: 0,
            errorDetails: []
        };

        const batches = [];
        for (let i = 0; i < products.length; i += this.batchConfig.batchSize) {
            batches.push(products.slice(i, i + this.batchConfig.batchSize));
        }

        logger.info(`Created ${batches.length} batches of ${this.batchConfig.batchSize} products each`);

        // Process batches with limited concurrency
        for (let i = 0; i < batches.length; i += this.batchConfig.maxConcurrentBatches) {
            const concurrentBatches = batches.slice(i, i + this.batchConfig.maxConcurrentBatches);
            
            const batchPromises = concurrentBatches.map(async (batch, batchIndex) => {
                const globalBatchIndex = i + batchIndex + 1;
                try {
                    return await this.processSingleBatch(batch, globalBatchIndex, batches.length);
                } catch (error) {
                    logger.error(`Batch ${globalBatchIndex} promise failed:`, error.message);
                    logger.error('Stack trace:', error.stack);
                    return {
                        processed: 0,
                        created: 0,
                        errors: batch.length,
                        errorDetails: [{
                            batch: globalBatchIndex,
                            error: error.message,
                            items: batch.map(item => item.Item).join(', ')
                        }]
                    };
                }
            });

            logger.info(`Processing ${concurrentBatches.length} batches concurrently (batches ${i + 1}-${Math.min(i + this.batchConfig.maxConcurrentBatches, batches.length)})`);

            const batchResults = await Promise.allSettled(batchPromises);

            // Aggregate results
            batchResults.forEach((result, index) => {
                if (result.status === 'fulfilled') {
                    const batchResult = result.value;
                    results.totalProcessed += batchResult.processed;
                    results.created += batchResult.created;
                    results.errors += batchResult.errors;
                    results.errorDetails.push(...batchResult.errorDetails);
                } else {
                    const batchIndex = i + index + 1;
                    logger.error(`Batch ${batchIndex} failed:`, result.reason?.message);
                    results.errors += this.batchConfig.batchSize; // Assume all products in batch failed
                    results.errorDetails.push({
                        batch: batchIndex,
                        error: result.reason?.message || 'Batch processing failed'
                    });
                }
            });

            // Delay between batch groups (except for the last group)
            if (i + this.batchConfig.maxConcurrentBatches < batches.length) {
                logger.info(`Waiting ${this.batchConfig.delayBetweenBatches}ms before next batch group...`);
                await new Promise(resolve => setTimeout(resolve, this.batchConfig.delayBetweenBatches));
            }
        }

        return results;
    }

    /**
     * Process a single batch of products
     */
    async processSingleBatch(batch, batchIndex, totalBatches) {
        const batchResult = {
            processed: 0,
            created: 0,
            errors: 0,
            errorDetails: []
        };

        logger.info(`Processing batch ${batchIndex}/${totalBatches} (${batch.length} products)`);

        try {
            // Use the new simplified batch processing method
            const shopifyResult = await this.shopifyClient.processBatchDirect(batch, {
                dryRun: false
            });

            batchResult.processed = batch.length;
            batchResult.created = shopifyResult.created;
            batchResult.errors = shopifyResult.errors;
            batchResult.errorDetails = shopifyResult.errorDetails || [];

            logger.info(`✓ Batch ${batchIndex} completed: ${shopifyResult.created} created, ${shopifyResult.errors} errors`);

        } catch (error) {
            logger.error(`✗ Batch ${batchIndex} failed:`, error.message);
            batchResult.errors = batch.length;
            batchResult.errorDetails = [{
                batch: batchIndex,
                error: error.message,
                items: batch.map(item => item.Item).join(', ')
            }];
        }

        return batchResult;
    }

    /**
     * Main processing flow - now processes all filtered products in batches
     */
    async run() {
        return await this.processFilteredProductsBatch();
    }

    /**
     * Legacy method - process single product (for testing)
     */
    async runSingle() {
        try {
            logger.info('=== RUNNING SINGLE PRODUCT MODE ===');
            
            // Step 1: Initialize Shopify client
            logger.info('Step 1: Initializing Shopify client...');
            await this.shopifyClient.initialize();
            logger.info('✓ Shopify client initialized successfully');

            // Step 2: Test Shopify connection
            logger.info('Step 2: Testing Shopify connection...');
            await this.shopifyClient.testConnection();
            logger.info('✓ Shopify connection test successful');

            // Step 3: Process CSV file (first product only)
            logger.info('Step 3: Processing CSV file...');
            const csvResult = await this.csvProcessor.processFile(path.join(__dirname, '..', 'data', 'partial.csv'));
            logger.info(`✓ CSV processed: ${csvResult.stats.processedRows} products loaded`);

            if (csvResult.data.length === 0) {
                logger.warn('No products found in CSV file');
                return;
            }

            // Step 4: Get the first product only
            logger.info('Step 4: Selecting first product for processing...');
            const firstProduct = csvResult.data[0];
            logger.info(`✓ Selected product: ${firstProduct.Description || firstProduct.Item} (SKU: ${firstProduct.Item})`);

            // Step 5: Process the single product in Shopify
            logger.info('Step 5: Creating product in Shopify...');
            
            // You can set dryRun to true to test without actually creating the product
            const dryRun = false; // Set to true for testing
            
            const result = await this.shopifyClient.processSingleProductFromCSV(firstProduct, { dryRun });
            
            if (result.success) {
                if (dryRun) {
                    logger.info('✓ Dry run completed successfully - no product was actually created');
                    logger.info('Product data that would be sent:', JSON.stringify(result.productData, null, 2));
                } else {
                    logger.info('✓ Product created successfully in Shopify');
                    logger.info('Created product:', JSON.stringify(result.result, null, 2));
                }
            } else {
                logger.error('✗ Failed to create product:', result.error);
                throw new Error(result.error);
            }

            logger.info('=== SINGLE PRODUCT PROCESSING COMPLETED SUCCESSFULLY ===');

        } catch (error) {
            logger.error('=== SINGLE PRODUCT PROCESSING FAILED ===');
            logger.error('Error:', error.message);
            if (error.stack) {
                logger.error('Stack trace:', error.stack);
            }
            throw error;
        }
    }

    /**
     * Process with dry run mode (for testing)
     */
    async testRun() {
        try {
            logger.info('=== RUNNING IN TEST MODE (DRY RUN) ===');
            
            // Initialize Shopify client
            await this.shopifyClient.initialize();
            
            // Process CSV
            const csvResult = await this.csvProcessor.processFile(this.csvFilePath);
            logger.info(`✓ CSV processed: ${csvResult.stats.processedRows} total products loaded`);
            
            // Apply filters
            const filteredProducts = this.filterProducts(csvResult.data);
            logger.info(`✓ Filtered products: ${filteredProducts.length} products match criteria`);
            
            if (filteredProducts.length === 0) {
                logger.warn('No products match the filter criteria');
                return;
            }

            // Test with just first 3 products
            const testProducts = filteredProducts.slice(0, 3);
            logger.info(`Testing with first ${testProducts.length} filtered products:`);
            
            for (let i = 0; i < testProducts.length; i++) {
                const product = testProducts[i];
                logger.info(`${i + 1}. ${product.Description} (${product.Item})`);
                logger.info(`   MSRP: $${product.MSRP}, Product Line: ${product.ProductLine}`);
            }

            // Test batch processing with dry run
            const result = await this.shopifyClient.processBatchDirect(testProducts, { dryRun: true });
            
            if (result) {
                logger.info('✓ Test completed successfully');
                logger.info(`Results: ${result.created} would be created, ${result.errors} errors`);
                if (result.errors > 0) {
                    logger.info('Error details:', result.errorDetails);
                }
            } else {
                logger.error('✗ Test failed: No result returned');
            }

            return result;

        } catch (error) {
            logger.error('Test run failed:', error.message);
            logger.error('Stack trace:', error.stack);
            throw error;
        }
    }

    /**
     * Get product preview without processing - now shows filter analysis
     */
    async previewProduct() {
        try {
            logger.info('=== PREVIEWING PRODUCTS WITH FILTER ANALYSIS ===');
            
            const csvResult = await this.csvProcessor.processFile(this.csvFilePath);
            logger.info(`Total products in CSV: ${csvResult.data.length}`);

            if (csvResult.data.length === 0) {
                logger.warn('No products found in CSV file');
                return null;
            }

            // Apply filters and show analysis
            const filteredProducts = this.filterProducts(csvResult.data);
            logger.info(`Products matching filter criteria: ${filteredProducts.length}`);

            // Show filter criteria
            logger.info('Filter criteria:');
            logger.info(`- MSRP range: $${this.filterCriteria.msrpMin} - $${this.filterCriteria.msrpMax}`);
            logger.info(`- Product lines: ${this.filterCriteria.productLines.join(', ')}`);

            // Show sample of filtered products
            const sampleSize = Math.min(5, filteredProducts.length);
            if (sampleSize > 0) {
                logger.info(`\nFirst ${sampleSize} filtered products:`);
                for (let i = 0; i < sampleSize; i++) {
                    const product = filteredProducts[i];
                    logger.info(`${i + 1}. ${product.Description} (${product.Item})`);
                    logger.info(`   MSRP: $${product.MSRP}, Product Line: ${product.ProductLine}`);
                    logger.info(`   Categories: ${product.Categories}`);
                    logger.info('   ---');
                }

                // Show Shopify mapping for first product
                await this.shopifyClient.initialize();
                const shopifyProduct = this.shopifyClient.mapCSVToShopifyProduct(filteredProducts[0]);
                logger.info('\nShopify product structure for first filtered product:');
                logger.info(JSON.stringify(shopifyProduct, null, 2));
            }

            return { 
                totalProducts: csvResult.data.length,
                filteredProducts: filteredProducts.length,
                sampleProducts: filteredProducts.slice(0, sampleSize)
            };

        } catch (error) {
            logger.error('Preview failed:', error.message);
            throw error;
        }
    }

    /**
     * Test creating a single filtered product (for debugging)
     */
    async testSingleCreate() {
        try {
            logger.info('=== TESTING SINGLE PRODUCT CREATION ===');
            
            // Initialize Shopify client
            await this.shopifyClient.initialize();
            
            // Process CSV and get filtered products
            const csvResult = await this.csvProcessor.processFile(this.csvFilePath);
            const filteredProducts = this.filterProducts(csvResult.data);
            
            if (filteredProducts.length === 0) {
                logger.warn('No products match the filter criteria');
                return;
            }

            const testProduct = filteredProducts[0];
            logger.info(`Testing with: ${testProduct.Description} (${testProduct.Item})`);
            logger.info(`MSRP: $${testProduct.MSRP}, Product Line: ${testProduct.ProductLine}`);

            // Test product creation
            const result = await this.shopifyClient.processSingleProductFromCSV(testProduct, { dryRun: false });
            
            if (result.success) {
                logger.info('✓ Single product creation test successful');
                logger.info('Created product details:', JSON.stringify(result.result, null, 2));
            } else {
                logger.error('✗ Single product creation test failed:', result.error);
            }

            return result;

        } catch (error) {
            logger.error('Single create test failed:', error.message);
            logger.error('Stack trace:', error.stack);
            throw error;
        }
    }
}

/**
 * CLI interface
 */
async function main() {
    const processor = new ProductProcessor();
    
    // Check command line arguments
    const args = process.argv.slice(2);
    const command = args[0] || 'run';

    try {
        switch (command) {
            case 'test':
                await processor.testRun();
                break;
            case 'test-single':
                await processor.testSingleCreate();
                break;
            case 'preview':
                await processor.previewProduct();
                break;
            case 'single':
                await processor.runSingle();
                break;
            case 'run':
            case 'batch':
            default:
                await processor.run();
                break;
        }
    } catch (error) {
        logger.error('Application failed:', error.message);
        process.exit(1);
    }
}

// Export the class for use in other modules
module.exports = ProductProcessor;

// Run if called directly
if (require.main === module) {
    main();
}
