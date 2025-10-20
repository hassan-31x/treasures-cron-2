const CSVProcessor = require('./src/csvProcessor');
const logger = require('./src/logger');

class ProductAnalyzer {
  constructor() {
    this.csvProcessor = new CSVProcessor();
  }

  /**
   * Analyze products from CSV with filtering criteria
   * @param {string} csvFilePath - Path to the CSV file
   * @param {object} options - Analysis options
   */
  async analyzeProductsFromCSV(csvFilePath, options = {}) {
    const {
      msrpMin = 1000,
      msrpMax = 2000,
      productLines = [
        'BRACELET',
        'EARRINGS',
        'NECKLACE',
        'PENDANT',
        'RING',
        'CHARM',
        'ANKLET',
        'CHAIN',
        'SET'
      ]
    } = options;

    logger.info(`=== PRODUCT ANALYSIS ===`);
    logger.info(`CSV File: ${csvFilePath}`);
    logger.info(`MSRP Range: $${msrpMin} - $${msrpMax}`);
    logger.info(`Product Lines: ${productLines.join(', ')}`);
    logger.info('='.repeat(50));

    try {
      // Get file information
      const fileInfo = await this.csvProcessor.getFileInfo(csvFilePath);
      logger.info(`✓ CSV file found: ${fileInfo.exists ? 'Yes' : 'No'}`);
      if (fileInfo.exists) {
        logger.info(`  File size: ${(fileInfo.sizeInBytes / 1024 / 1024).toFixed(2)} MB`);
        logger.info(`  Estimated rows: ~${fileInfo.estimatedRowCount}`);
      }

      // Process CSV file
      logger.info('\nProcessing CSV file...');
      const csvData = await this.csvProcessor.processFile(csvFilePath);
      logger.info(`✓ Loaded ${csvData.length} total products from CSV`);

      // Analyze all products
      const analysis = this.performAnalysis(csvData, { msrpMin, msrpMax, productLines });

      // Log results
      this.logAnalysisResults(analysis);

      return analysis;

    } catch (error) {
      logger.error('Error in analyzeProductsFromCSV:', error.message);
      throw error;
    }
  }

  /**
   * Perform detailed analysis on the CSV data
   */
  performAnalysis(csvData, criteria) {
    const { msrpMin, msrpMax, productLines } = criteria;

    const analysis = {
      total: csvData.length,
      matching: 0,
      rejected: 0,
      rejectionReasons: {},
      matchingSample: [],
      priceDistribution: {},
      productLineDistribution: {},
      statusDistribution: {},
      errors: []
    };

    // Initialize rejection reasons
    analysis.rejectionReasons = {
      'No MSRP data': 0,
      'MSRP below minimum': 0,
      'MSRP above maximum': 0,
      'Product line not in criteria': 0,
      'Multiple criteria failed': 0
    };

    csvData.forEach((item, index) => {
      try {
        const result = this.evaluateProduct(item, criteria);
        
        if (result.matches) {
          analysis.matching++;
          
          // Add to sample (keep first 10 matching products)
          if (analysis.matchingSample.length < 10) {
            analysis.matchingSample.push({
              item: item.Item,
              description: item.Description,
              msrp: item.MSRP,
              productLine: item.ProductLine,
              status: item.Status,
              categories: item.Categories
            });
          }

          // Update distributions for matching products
          this.updateDistributions(item, analysis);
        } else {
          analysis.rejected++;
          
          // Count rejection reasons
          if (result.reasons.length === 1) {
            analysis.rejectionReasons[result.reasons[0]]++;
          } else if (result.reasons.length > 1) {
            analysis.rejectionReasons['Multiple criteria failed']++;
          }
        }

      } catch (error) {
        analysis.errors.push({
          index,
          item: item.Item || 'Unknown',
          error: error.message
        });
      }
    });

    return analysis;
  }

  /**
   * Evaluate if a single product matches the criteria
   */
  evaluateProduct(item, criteria) {
    const { msrpMin, msrpMax, productLines } = criteria;
    const reasons = [];
    let matches = true;

    // Check MSRP
    const msrpValue = this.parsePrice(item.MSRP);
    if (msrpValue === null) {
      reasons.push('No MSRP data');
      matches = false;
    } else if (msrpValue < msrpMin) {
      reasons.push('MSRP below minimum');
      matches = false;
    } else if (msrpValue > msrpMax) {
      reasons.push('MSRP above maximum');
      matches = false;
    }

    // Check product line
    const productLine = (item.ProductLine || '').toUpperCase().trim();
    if (!productLine || !productLines.includes(productLine)) {
      reasons.push('Product line not in criteria');
      matches = false;
    }

    return { matches, reasons, msrp: msrpValue, productLine };
  }

  /**
   * Update distribution counters for matching products
   */
  updateDistributions(item, analysis) {
    // Price distribution (in $100 buckets)
    const msrp = this.parsePrice(item.MSRP);
    if (msrp !== null) {
      const bucket = Math.floor(msrp / 100) * 100;
      const bucketKey = `$${bucket}-${bucket + 99}`;
      analysis.priceDistribution[bucketKey] = (analysis.priceDistribution[bucketKey] || 0) + 1;
    }

    // Product line distribution
    const productLine = (item.ProductLine || 'Unknown').toUpperCase().trim();
    analysis.productLineDistribution[productLine] = (analysis.productLineDistribution[productLine] || 0) + 1;

    // Status distribution
    const status = item.Status || 'Unknown';
    analysis.statusDistribution[status] = (analysis.statusDistribution[status] || 0) + 1;
  }

  /**
   * Parse price from string (same logic as ShopifyClient)
   */
  parsePrice(priceStr) {
    if (!priceStr) return null;
    const price = parseFloat(priceStr.toString().replace(/[^\d.-]/g, ''));
    return isNaN(price) ? null : price;
  }

  /**
   * Log comprehensive analysis results
   */
  logAnalysisResults(analysis) {
    logger.info('\n' + '='.repeat(50));
    logger.info(`=== ANALYSIS RESULTS ===`);
    logger.info(`Total products analyzed: ${analysis.total}`);
    logger.info(`Products matching criteria: ${analysis.matching}`);
    logger.info(`Products rejected: ${analysis.rejected}`);
    
    const matchPercentage = analysis.total > 0 ? ((analysis.matching / analysis.total) * 100).toFixed(1) : 0;
    logger.info(`Match rate: ${matchPercentage}%`);

    // Rejection reasons
    if (analysis.rejected > 0) {
      logger.info('\nRejection breakdown:');
      Object.entries(analysis.rejectionReasons).forEach(([reason, count]) => {
        if (count > 0) {
          const percentage = ((count / analysis.rejected) * 100).toFixed(1);
          logger.info(`  ${reason}: ${count} (${percentage}% of rejected)`);
        }
      });
    }

    // Product line distribution
    if (Object.keys(analysis.productLineDistribution).length > 0) {
      logger.info('\nMatching products by product line:');
      const sortedLines = Object.entries(analysis.productLineDistribution)
        .sort(([,a], [,b]) => b - a);
      
      sortedLines.forEach(([line, count]) => {
        const percentage = ((count / analysis.matching) * 100).toFixed(1);
        logger.info(`  ${line}: ${count} (${percentage}%)`);
      });
    }

    // Price distribution
    if (Object.keys(analysis.priceDistribution).length > 0) {
      logger.info('\nMatching products by price range:');
      const sortedPrices = Object.entries(analysis.priceDistribution)
        .sort(([a], [b]) => {
          const aValue = parseInt(a.split('-')[0].replace('$', ''));
          const bValue = parseInt(b.split('-')[0].replace('$', ''));
          return aValue - bValue;
        });
      
      sortedPrices.forEach(([range, count]) => {
        const percentage = ((count / analysis.matching) * 100).toFixed(1);
        logger.info(`  ${range}: ${count} (${percentage}%)`);
      });
    }

    // Status distribution
    if (Object.keys(analysis.statusDistribution).length > 0) {
      logger.info('\nMatching products by status:');
      Object.entries(analysis.statusDistribution).forEach(([status, count]) => {
        const percentage = ((count / analysis.matching) * 100).toFixed(1);
        logger.info(`  ${status}: ${count} (${percentage}%)`);
      });
    }

    // Sample matching products
    if (analysis.matchingSample.length > 0) {
      logger.info(`\nSample matching products (showing ${analysis.matchingSample.length}):`);
      analysis.matchingSample.forEach((product, index) => {
        logger.info(`${index + 1}. ${product.description || product.item}`);
        logger.info(`   Item: ${product.item}`);
        logger.info(`   MSRP: $${product.msrp}`);
        logger.info(`   Product Line: ${product.productLine}`);
        logger.info(`   Status: ${product.status}`);
        logger.info('   ---');
      });
    }

    // Errors
    if (analysis.errors.length > 0) {
      logger.warn(`\nErrors encountered: ${analysis.errors.length}`);
      analysis.errors.slice(0, 5).forEach((error, index) => {
        logger.error(`${index + 1}. Item ${error.item}: ${error.error}`);
      });
      if (analysis.errors.length > 5) {
        logger.info(`... and ${analysis.errors.length - 5} more errors`);
      }
    }

    logger.info('='.repeat(50));
  }
}

// Main execution function
async function main() {
  const analyzer = new ProductAnalyzer();

  try {
    // Configuration - same filtering criteria as the main processing script
    const csvFilePath = './data/products.csv';
    const filteringCriteria = {
      msrpMin: 1000,
      msrpMax: 2000,
      productLines: [
        'BRACELET',
        'EARRINGS', 
        'NECKLACE',
        'PENDANT',
        'RING',
        'CHARM',
        'ANKLET',
        'CHAIN',
        'SET'
      ]
    };

    // Process command line arguments for custom criteria
    const args = process.argv.slice(2);
    
    // Custom MSRP range
    const minIndex = args.indexOf('--min');
    if (minIndex !== -1 && args[minIndex + 1]) {
      filteringCriteria.msrpMin = parseFloat(args[minIndex + 1]);
    }
    
    const maxIndex = args.indexOf('--max');
    if (maxIndex !== -1 && args[maxIndex + 1]) {
      filteringCriteria.msrpMax = parseFloat(args[maxIndex + 1]);
    }

    // Custom CSV file
    const fileIndex = args.indexOf('--file');
    if (fileIndex !== -1 && args[fileIndex + 1]) {
      csvFilePath = args[fileIndex + 1];
    }

    // Execute analysis
    const results = await analyzer.analyzeProductsFromCSV(csvFilePath, filteringCriteria);

    // Summary for quick reference
    logger.info(`\n🎯 SUMMARY: ${results.matching} products match your criteria out of ${results.total} total products`);
    
    process.exit(0);

  } catch (error) {
    logger.error('Analysis failed:', error.message);
    process.exit(1);
  }
}

// Run if this file is executed directly
if (require.main === module) {
  main();
}

module.exports = ProductAnalyzer;