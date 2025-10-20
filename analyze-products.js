#!/usr/bin/env node

/**
 * Product Count Analyzer
 * Analyzes the products.csv file and counts products that match the filtering criteria
 */

const CSVProcessor = require('./src/csvProcessor');
const logger = require('./src/logger');
const path = require('path');

class ProductAnalyzer {
    constructor() {
        this.csvProcessor = new CSVProcessor();
        this.csvFilePath = path.join(__dirname, 'data', 'products.csv');
        
        // Same filtering criteria as the main application
        this.filterCriteria = {
            msrpMin: 1000,
            msrpMax: 2000,
            productLines: [
                'Bridal',
                'Fancy Diamond Hoops', 
                'Fine Diamond Jewelry',
                'Wedding Bands USA',
                'earrings',
                'bracelets', 
                'chains',
                'pendants',
                'engagement rings',
                'wedding bands'
            ]
        };
    }

    /**
     * Filter products based on MSRP and Product Line criteria (same as main app)
     */
    filterProducts(products) {
        return products.filter(product => {
            // Check MSRP criteria
            const msrp = parseFloat(product.ContractPrice);
            if (isNaN(msrp) || msrp < this.filterCriteria.msrpMin || msrp > this.filterCriteria.msrpMax) {
                return false;
            }

            // Check Product Line criteria
            const productLine = (product.ProductLine || '').toLowerCase().trim();
            const hasMatchingProductLine = this.filterCriteria.productLines.some(line => {
                const lineLower = line.toLowerCase();
                // Exact match or contains the keyword
                return productLine === lineLower || productLine.includes(lineLower);
            });

            if (!hasMatchingProductLine) {
                return false;
            }

            // Additional quality checks
            if (!product.Item || !product.Description) {
                return false;
            }

            return true;
        });
    }

    /**
     * Analyze products and provide detailed breakdown
     */
    async analyzeProducts() {
        try {
            logger.info('=== PRODUCT COUNT ANALYSIS ===');
            logger.info(`Reading from: ${this.csvFilePath}`);
            logger.info(`Filter criteria:`);
            logger.info(`- MSRP range: $${this.filterCriteria.msrpMin} - $${this.filterCriteria.msrpMax}`);
            logger.info(`- Product lines: ${this.filterCriteria.productLines.join(', ')}`);
            logger.info('='.repeat(50));

            // Step 1: Get file information
            const fileInfo = await this.csvProcessor.getFileInfo(this.csvFilePath);
            logger.info(`CSV file: ${fileInfo.sizeMB}MB, estimated ${fileInfo.estimatedRows} rows`);

            // Step 2: Process CSV file
            logger.info('Processing CSV file...');
            const csvResult = await this.csvProcessor.processFile(this.csvFilePath);
            logger.info(`✓ CSV processed: ${csvResult.stats.processedRows} total products loaded`);

            // Step 3: Apply filters and analyze
            logger.info('Applying filters and analyzing...');
            const filteredProducts = this.filterProducts(csvResult.data);

            // Step 4: Generate detailed analysis
            const analysis = this.generateAnalysis(csvResult.data, filteredProducts);
            
            // Step 5: Display results
            this.displayResults(analysis);

            return analysis;

        } catch (error) {
            logger.error('Analysis failed:', error.message);
            throw error;
        }
    }

    /**
     * Generate detailed analysis of the filtering results
     */
    generateAnalysis(allProducts, filteredProducts) {
        const analysis = {
            total: allProducts.length,
            filtered: filteredProducts.length,
            percentage: ((filteredProducts.length / allProducts.length) * 100).toFixed(2),
            msrpBreakdown: {},
            productLineBreakdown: {},
            sampleProducts: filteredProducts.slice(0, 10),
            priceStats: {},
            qualityStats: {}
        };

        // MSRP range breakdown
        const msrpRanges = {
            '1000-1200': 0,
            '1201-1400': 0,
            '1401-1600': 0,
            '1601-1800': 0,
            '1801-2000': 0
        };

        // Product line breakdown
        const productLineCounts = {};
        this.filterCriteria.productLines.forEach(line => {
            productLineCounts[line] = 0;
        });

        // Price statistics
        const prices = [];

        // Analyze filtered products
        filteredProducts.forEach(product => {
            const msrp = parseFloat(product.MSRP);
            prices.push(msrp);

            // MSRP range breakdown
            if (msrp >= 1000 && msrp <= 1200) msrpRanges['1000-1200']++;
            else if (msrp > 1200 && msrp <= 1400) msrpRanges['1201-1400']++;
            else if (msrp > 1400 && msrp <= 1600) msrpRanges['1401-1600']++;
            else if (msrp > 1600 && msrp <= 1800) msrpRanges['1601-1800']++;
            else if (msrp > 1800 && msrp <= 2000) msrpRanges['1801-2000']++;

            // Product line breakdown
            const productLine = (product.ProductLine || '').toLowerCase().trim();
            this.filterCriteria.productLines.forEach(line => {
                const lineLower = line.toLowerCase();
                if (productLine === lineLower || productLine.includes(lineLower)) {
                    productLineCounts[line]++;
                }
            });
        });

        // Calculate price statistics
        if (prices.length > 0) {
            prices.sort((a, b) => a - b);
            analysis.priceStats = {
                min: prices[0],
                max: prices[prices.length - 1],
                avg: (prices.reduce((sum, price) => sum + price, 0) / prices.length).toFixed(2),
                median: prices[Math.floor(prices.length / 2)]
            };
        }

        // Quality statistics
        let hasImages = 0;
        let hasSpecs = 0;
        let activeProducts = 0;

        filteredProducts.forEach(product => {
            if (product.ImageLink_1000) hasImages++;
            if (product.ListOfSpecs) hasSpecs++;
            if (product.Status === 'Active') activeProducts++;
        });

        analysis.qualityStats = {
            hasImages: hasImages,
            hasImagesPercentage: ((hasImages / filteredProducts.length) * 100).toFixed(1),
            hasSpecs: hasSpecs,
            hasSpecsPercentage: ((hasSpecs / filteredProducts.length) * 100).toFixed(1),
            activeProducts: activeProducts,
            activePercentage: ((activeProducts / filteredProducts.length) * 100).toFixed(1)
        };

        analysis.msrpBreakdown = msrpRanges;
        analysis.productLineBreakdown = productLineCounts;

        return analysis;
    }

    /**
     * Display analysis results in a formatted way
     */
    displayResults(analysis) {
        logger.info('='.repeat(50));
        logger.info('=== ANALYSIS RESULTS ===');
        logger.info('='.repeat(50));

        // Overall numbers
        logger.info(`📊 OVERALL STATISTICS:`);
        logger.info(`   Total products in CSV: ${analysis.total.toLocaleString()}`);
        logger.info(`   Products matching criteria: ${analysis.filtered.toLocaleString()}`);
        logger.info(`   Percentage of total: ${analysis.percentage}%`);
        logger.info('');

        // Price statistics
        if (analysis.priceStats.min) {
            logger.info(`💰 PRICE STATISTICS:`);
            logger.info(`   Minimum MSRP: $${analysis.priceStats.min}`);
            logger.info(`   Maximum MSRP: $${analysis.priceStats.max}`);
            logger.info(`   Average MSRP: $${analysis.priceStats.avg}`);
            logger.info(`   Median MSRP: $${analysis.priceStats.median}`);
            logger.info('');
        }

        // MSRP breakdown
        logger.info(`📈 MSRP RANGE BREAKDOWN:`);
        Object.entries(analysis.msrpBreakdown).forEach(([range, count]) => {
            const percentage = ((count / analysis.filtered) * 100).toFixed(1);
            logger.info(`   $${range}: ${count.toLocaleString()} products (${percentage}%)`);
        });
        logger.info('');

        // Product line breakdown
        logger.info(`🏷️  PRODUCT LINE BREAKDOWN:`);
        Object.entries(analysis.productLineBreakdown)
            .sort(([,a], [,b]) => b - a) // Sort by count descending
            .forEach(([line, count]) => {
                if (count > 0) {
                    const percentage = ((count / analysis.filtered) * 100).toFixed(1);
                    logger.info(`   ${line}: ${count.toLocaleString()} products (${percentage}%)`);
                }
            });
        logger.info('');

        // Quality statistics
        logger.info(`✅ QUALITY STATISTICS:`);
        logger.info(`   Products with images: ${analysis.qualityStats.hasImages.toLocaleString()} (${analysis.qualityStats.hasImagesPercentage}%)`);
        logger.info(`   Products with specifications: ${analysis.qualityStats.hasSpecs.toLocaleString()} (${analysis.qualityStats.hasSpecsPercentage}%)`);
        logger.info(`   Active products: ${analysis.qualityStats.activeProducts.toLocaleString()} (${analysis.qualityStats.activePercentage}%)`);
        logger.info('');

        // Sample products
        if (analysis.sampleProducts.length > 0) {
            logger.info(`📋 SAMPLE PRODUCTS (first 10):`);
            analysis.sampleProducts.forEach((product, index) => {
                logger.info(`   ${index + 1}. ${product.Description} (${product.Item})`);
                logger.info(`      MSRP: $${product.MSRP} | Line: ${product.ProductLine} | Status: ${product.Status}`);
            });
            logger.info('');
        }

        // Processing estimates
        logger.info(`⏱️  PROCESSING ESTIMATES:`);
        const batchSize = 10;
        const totalBatches = Math.ceil(analysis.filtered / batchSize);
        const estimatedMinutes = Math.ceil(totalBatches * 0.5); // Assuming 30 seconds per batch
        
        logger.info(`   Total batches needed: ${totalBatches.toLocaleString()}`);
        logger.info(`   Estimated processing time: ${estimatedMinutes} minutes`);
        logger.info(`   API calls needed: ${(analysis.filtered * 2).toLocaleString()} (REST + GraphQL per product)`);
        
        logger.info('='.repeat(50));
        logger.info('Analysis complete! ✅');
    }
}

/**
 * Main execution function
 */
async function main() {
    try {
        const analyzer = new ProductAnalyzer();
        const analysis = await analyzer.analyzeProducts();
        
        // Optionally save results to file
        const fs = require('fs');
        const resultsFile = 'analysis-results.json';
        fs.writeFileSync(resultsFile, JSON.stringify(analysis, null, 2));
        logger.info(`\n📄 Detailed results saved to: ${resultsFile}`);
        
    } catch (error) {
        logger.error('❌ Analysis failed:', error.message);
        process.exit(1);
    }
}

// Export for use in other modules
module.exports = ProductAnalyzer;

// Run if called directly
if (require.main === module) {
    main();
}