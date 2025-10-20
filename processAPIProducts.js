const axios = require('axios');
const ShopifyClient = require('./src/shopifyClient');
const logger = require('./src/logger');

class OvernightMountingsAPIProcessor {
  constructor() {
    this.shopifyClient = new ShopifyClient();
    this.baseApiUrl = 'https://connect.overnightmountings.com/api/rest/instockitem';
    this.defaultParams = {
      number_of_items: 30,
      category_id: 1200
    };
    
    // Filtering criteria (same as your main application)
    this.filterCriteria = {
      priceMin: 1000,
      priceMax: 2000,
      productClasses: [
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
  }

  /**
   * Process products from Overnight Mountings API
   * @param {object} options - Processing options
   */
  async processProductsFromAPI(options = {}) {
    const {
      startPage = 1,
      maxPages = null,
      batchSize = 10,
      delayBetweenBatches = 2000,
      delayBetweenPages = 1000,
      dryRun = false,
      enableFiltering = true
    } = options;

    logger.info(`=== OVERNIGHT MOUNTINGS API PROCESSOR ===`);
    logger.info(`Starting from page: ${startPage}`);
    logger.info(`Max pages: ${maxPages || 'unlimited'}`);
    logger.info(`Batch size: ${batchSize}`);
    logger.info(`Filtering enabled: ${enableFiltering}`);
    logger.info(`Dry run: ${dryRun}`);
    logger.info('='.repeat(50));

    const results = {
      totalFetched: 0,
      totalFiltered: 0,
      totalProcessed: 0,
      created: 0,
      updated: 0,
      errors: 0,
      pagesProcessed: 0,
      errorDetails: []
    };

    try {
      // Initialize Shopify client
      await this.shopifyClient.initialize();
      logger.info('✓ Shopify client initialized');

      let currentPage = startPage;
      let hasMorePages = true;
      let consecutiveEmptyPages = 0;

      while (hasMorePages) {
        logger.info(`\n--- Processing page ${currentPage} ---`);

        try {
          // Fetch products from API
          const pageData = await this.fetchPageFromAPI(currentPage);
          
          if (!pageData || Object.keys(pageData).length === 0) {
            consecutiveEmptyPages++;
            logger.warn(`Page ${currentPage} is empty (${consecutiveEmptyPages} consecutive empty pages)`);
            
            // Stop if we hit 3 consecutive empty pages
            if (consecutiveEmptyPages >= 3) {
              logger.info('Reached 3 consecutive empty pages, stopping pagination');
              break;
            }
          } else {
            consecutiveEmptyPages = 0;
            const products = Object.values(pageData);
            results.totalFetched += products.length;
            results.pagesProcessed++;

            logger.info(`✓ Fetched ${products.length} products from page ${currentPage}`);

            // Filter products if enabled
            let productsToProcess = products;
            if (enableFiltering) {
              productsToProcess = this.filterProducts(products);
              results.totalFiltered += productsToProcess.length;
              logger.info(`✓ ${productsToProcess.length} products passed filtering criteria`);
            } else {
              results.totalFiltered += productsToProcess.length;
            }

            // Process products in batches
            if (productsToProcess.length > 0) {
              const batchResults = await this.processProductsBatch(productsToProcess, {
                batchSize,
                delayBetweenBatches,
                dryRun
              });

              // Update results
              results.totalProcessed += batchResults.total;
              results.created += batchResults.created;
              results.updated += batchResults.updated;
              results.errors += batchResults.errors;
              results.errorDetails.push(...batchResults.errorDetails);

              logger.info(`✓ Page ${currentPage} processed: ${batchResults.created} created, ${batchResults.updated} updated, ${batchResults.errors} errors`);
            } else {
              logger.info(`No products to process from page ${currentPage}`);
            }
          }

          // Check stopping conditions
          currentPage++;
          if (maxPages && (currentPage - startPage) >= maxPages) {
            logger.info(`Reached maximum pages limit (${maxPages})`);
            break;
          }

          // Delay between pages to respect API rate limits
          if (delayBetweenPages > 0) {
            logger.debug(`Waiting ${delayBetweenPages}ms before next page...`);
            await this.delay(delayBetweenPages);
          }

        } catch (pageError) {
          logger.error(`Error processing page ${currentPage}:`, pageError.message);
          results.errors++;
          results.errorDetails.push({
            type: 'page_error',
            page: currentPage,
            error: pageError.message
          });

          // Continue to next page unless it's a critical error
          if (pageError.message.includes('404') || pageError.message.includes('unauthorized')) {
            logger.error('Critical API error, stopping pagination');
            break;
          }
          
          currentPage++;
        }
      }

      // Log final results
      this.logFinalResults(results);
      return results;

    } catch (error) {
      logger.error('Error in processProductsFromAPI:', error.message);
      throw error;
    }
  }

  /**
   * Fetch a single page of products from the API
   */
  async fetchPageFromAPI(pageNumber) {
    try {
      const params = {
        ...this.defaultParams,
        page_number: pageNumber
      };

      logger.debug(`Fetching: ${this.baseApiUrl} with params:`, params);

      const response = await axios.get(this.baseApiUrl, {
        params,
        timeout: 30000, // 30 second timeout
        headers: {
          'Accept': 'application/json',
          'User-Agent': 'Shopify-Integration-Script/1.0'
        }
      });

      if (response.status !== 200) {
        throw new Error(`API returned status ${response.status}: ${response.statusText}`);
      }

      return response.data;

    } catch (error) {
      if (error.response) {
        const status = error.response.status;
        const statusText = error.response.statusText;
        throw new Error(`API Error ${status}: ${statusText}`);
      } else if (error.code === 'ECONNABORTED') {
        throw new Error('API request timeout');
      } else if (error.code === 'ENOTFOUND' || error.code === 'ECONNREFUSED') {
        throw new Error('Cannot connect to API - check network connection');
      }
      throw error;
    }
  }

  /**
   * Filter products based on criteria (similar to CSV filtering)
   */
  filterProducts(products) {
    return products.filter(product => {
      try {
        // Check price criteria (using finalprice)
        const price = this.parsePrice(product.finalprice);
        if (price === null || price < this.filterCriteria.priceMin || price > this.filterCriteria.priceMax) {
          return false;
        }

        // Check product class criteria
        const productClass = (product.ProductClass || '').toUpperCase().trim();
        if (!productClass || !this.filterCriteria.productClasses.includes(productClass)) {
          return false;
        }

        // Check if product has required fields
        if (!product.sku || !product.name) {
          return false;
        }

        return true;

      } catch (error) {
        logger.warn(`Error filtering product ${product.sku || 'unknown'}:`, error.message);
        return false;
      }
    });
  }

  /**
   * Process products in batches
   */
  async processProductsBatch(products, options) {
    const { batchSize, delayBetweenBatches, dryRun } = options;

    const results = {
      total: products.length,
      created: 0,
      updated: 0,
      errors: 0,
      errorDetails: []
    };

    // Process in batches
    for (let i = 0; i < products.length; i += batchSize) {
      const batch = products.slice(i, i + batchSize);
      const batchNumber = Math.floor(i / batchSize) + 1;
      const totalBatches = Math.ceil(products.length / batchSize);

      logger.info(`  Processing batch ${batchNumber}/${totalBatches} (${batch.length} products)`);

      try {
        // Convert API products to Shopify format and process
        const shopifyProducts = batch.map(apiProduct => this.mapAPIToShopifyProduct(apiProduct));
        
        const batchResults = await this.shopifyClient.processBatchDirect(shopifyProducts, { dryRun });

        results.created += batchResults.created;
        results.errors += batchResults.errors;
        results.errorDetails.push(...batchResults.errorDetails);

        logger.info(`  Batch ${batchNumber} completed: ${batchResults.created} created, ${batchResults.errors} errors`);

      } catch (batchError) {
        logger.error(`Batch ${batchNumber} failed:`, batchError.message);
        results.errors += batch.length;
        batch.forEach(product => {
          results.errorDetails.push({
            item: product.sku,
            error: `Batch failure: ${batchError.message}`
          });
        });
      }

      // Delay between batches
      if (i + batchSize < products.length && delayBetweenBatches > 0) {
        await this.delay(delayBetweenBatches);
      }
    }

    return results;
  }

  /**
   * Map API product data to Shopify product format
   */
  mapAPIToShopifyProduct(apiProduct) {
    const product = {
      title: apiProduct.name || apiProduct.description || 'Untitled Product',
      handle: this.generateHandle(apiProduct.name || apiProduct.sku),
      body_html: this.generateProductDescription(apiProduct),
      vendor: 'Overnight Mountings',
      product_type: this.extractProductType(apiProduct),
      status: 'active',
      published: true,
      tags: this.generateTags(apiProduct),
      variants: [this.createVariant(apiProduct)],
      images: this.createImages(apiProduct),
      metafields: this.createMetafields(apiProduct),
      category: this.getCategoryId(apiProduct)
    };

    // Add SEO fields
    if (apiProduct.name) {
      product.seo_title = apiProduct.name.substring(0, 70);
      product.seo_description = this.generateSEODescription(apiProduct);
    }

    return product;
  }

  /**
   * Generate product description from API data
   */
  generateProductDescription(apiProduct) {
    let description = '';

    if (apiProduct.description && apiProduct.description !== apiProduct.name) {
      description += `<h3>${apiProduct.description}</h3>`;
    }

    if (apiProduct.metalType && apiProduct.metalColor) {
      description += `<p><strong>Metal:</strong> ${apiProduct.metalType} ${apiProduct.metalColor}</p>`;
    }

    if (apiProduct.metalWeight && apiProduct.WeightUnit) {
      description += `<p><strong>Weight:</strong> ${apiProduct.metalWeight} ${apiProduct.WeightUnit}</p>`;
    }

    if (apiProduct.TotalDiamondWeight) {
      description += `<p><strong>Diamond Weight:</strong> ${apiProduct.TotalDiamondWeight}</p>`;
    }

    if (apiProduct.diamondQuality) {
      description += `<p><strong>Diamond Quality:</strong> ${apiProduct.diamondQuality}</p>`;
    }

    if (apiProduct.SideDiamondNumber) {
      description += `<p><strong>Number of Diamonds:</strong> ${apiProduct.SideDiamondNumber}</p>`;
    }

    if (apiProduct.shippingDay) {
      description += `<p><strong>Shipping:</strong> ${apiProduct.shippingDay} day(s)</p>`;
    }

    return description || '<p>Quality jewelry piece from Overnight Mountings</p>';
  }

  /**
   * Create variant from API data
   */
  createVariant(apiProduct) {
    const variant = {
      title: 'Default Title',
      sku: apiProduct.sku || apiProduct.entity_id || '',
      price: this.parsePrice(apiProduct.finalprice) || '0.00',
      inventory_management: 'shopify',
      inventory_quantity: parseInt(apiProduct.qoh) || 0,
      weight: this.parseWeight(apiProduct.metalWeight),
      weight_unit: 'g'
    };

    // Add size if available
    if (apiProduct.FingerSize) {
      variant.option1 = apiProduct.FingerSize;
    }

    return variant;
  }

  /**
   * Create images from API data
   */
  createImages(apiProduct) {
    const images = [];

    // Default image
    if (apiProduct.default_image_url) {
      images.push({
        src: apiProduct.default_image_url,
        alt: apiProduct.name || apiProduct.sku
      });
    }

    // Additional images
    if (apiProduct.images && Array.isArray(apiProduct.images)) {
      apiProduct.images.forEach((imageUrl, index) => {
        if (imageUrl && imageUrl !== apiProduct.default_image_url) {
          images.push({
            src: imageUrl,
            alt: `${apiProduct.name || apiProduct.sku} - View ${index + 1}`
          });
        }
      });
    }

    return images;
  }

  /**
   * Create metafields for API-specific data
   */
  createMetafields(apiProduct) {
    const metafields = [];

    // Store API entity ID
    if (apiProduct.entity_id) {
      metafields.push({
        namespace: 'api_source',
        key: 'entity_id',
        value: apiProduct.entity_id,
        type: 'single_line_text_field'
      });
    }

    // Store metal specifications
    if (apiProduct.BaseMetalType) {
      metafields.push({
        namespace: 'specifications',
        key: 'base_metal_type',
        value: apiProduct.BaseMetalType,
        type: 'single_line_text_field'
      });
    }

    // Store gemstone information
    if (apiProduct.GemstoneType1) {
      metafields.push({
        namespace: 'specifications',
        key: 'primary_gemstone',
        value: apiProduct.GemstoneType1,
        type: 'single_line_text_field'
      });
    }

    return metafields;
  }

  /**
   * Extract product type from API data
   */
  extractProductType(apiProduct) {
    const productClass = (apiProduct.ProductClass || '').toLowerCase();
    const categoryValue = (apiProduct.categoryvalue || '').toLowerCase();

    if (productClass.includes('bracelet')) return 'Bracelets';
    if (productClass.includes('earring')) return 'Earrings';
    if (productClass.includes('necklace')) return 'Necklaces';
    if (productClass.includes('pendant')) return 'Pendants';
    if (productClass.includes('ring')) return 'Rings';
    if (productClass.includes('charm')) return 'Charms';
    if (productClass.includes('chain')) return 'Chains';
    
    return 'Jewelry';
  }

  /**
   * Generate tags from API data
   */
  generateTags(apiProduct) {
    const tags = [];

    if (apiProduct.metalType) tags.push(apiProduct.metalType);
    if (apiProduct.metalColor) tags.push(apiProduct.metalColor);
    if (apiProduct.ProductClass) tags.push(apiProduct.ProductClass);
    if (apiProduct.BaseMetalType) tags.push(apiProduct.BaseMetalType);
    if (apiProduct.diamondQuality) tags.push(apiProduct.diamondQuality);

    return tags.join(', ');
  }

  /**
   * Get category ID based on API product data
   */
  getCategoryId(apiProduct) {
    const productClass = (apiProduct.ProductClass || '').toLowerCase();
    
    // Use the same category mapping as ShopifyClient
    if (productClass.includes('bracelet')) return 'gid://shopify/TaxonomyCategory/aa-6-3';
    if (productClass.includes('earring')) return 'gid://shopify/TaxonomyCategory/aa-6-6';
    if (productClass.includes('necklace') || productClass.includes('chain')) return 'gid://shopify/TaxonomyCategory/aa-6-8';
    if (productClass.includes('pendant') || productClass.includes('charm')) return 'gid://shopify/TaxonomyCategory/aa-6-5';
    if (productClass.includes('ring')) return 'gid://shopify/TaxonomyCategory/aa-6-9';

    // Default to necklaces category
    return 'gid://shopify/TaxonomyCategory/aa-6-8';
  }

  /**
   * Generate URL handle from title
   */
  generateHandle(title) {
    if (!title) return 'product';
    return title
      .toLowerCase()
      .replace(/[^a-z0-9\s-]/g, '')
      .replace(/\s+/g, '-')
      .replace(/-+/g, '-')
      .replace(/^-|-$/g, '')
      .substring(0, 100);
  }

  /**
   * Generate SEO description
   */
  generateSEODescription(apiProduct) {
    const parts = [];
    if (apiProduct.name) parts.push(apiProduct.name);
    if (apiProduct.metalType && apiProduct.metalColor) parts.push(`${apiProduct.metalType} ${apiProduct.metalColor}`);
    if (apiProduct.TotalDiamondWeight) parts.push(`${apiProduct.TotalDiamondWeight} diamonds`);
    
    return parts.join(' - ').substring(0, 160);
  }

  /**
   * Parse price from string (handle comma-separated format)
   */
  parsePrice(priceStr) {
    if (!priceStr) return null;
    // Remove commas and parse
    const cleanPrice = priceStr.toString().replace(/[^\d.-]/g, '');
    const price = parseFloat(cleanPrice);
    return isNaN(price) ? null : price;
  }

  /**
   * Parse weight from string
   */
  parseWeight(weightStr) {
    if (!weightStr) return 0;
    const weight = parseFloat(weightStr.toString());
    return isNaN(weight) ? 0 : Math.round(weight * 1.55517); // Convert dwt to grams
  }

  /**
   * Log final results
   */
  logFinalResults(results) {
    logger.info('\n' + '='.repeat(50));
    logger.info(`=== API PROCESSING COMPLETE ===`);
    logger.info(`Pages processed: ${results.pagesProcessed}`);
    logger.info(`Total products fetched: ${results.totalFetched}`);
    logger.info(`Products after filtering: ${results.totalFiltered}`);
    logger.info(`Products processed: ${results.totalProcessed}`);
    logger.info(`Successfully created: ${results.created}`);
    logger.info(`Successfully updated: ${results.updated}`);
    logger.info(`Errors: ${results.errors}`);

    if (results.totalFetched > 0) {
      const filterRate = ((results.totalFiltered / results.totalFetched) * 100).toFixed(1);
      logger.info(`Filter pass rate: ${filterRate}%`);
    }

    if (results.totalProcessed > 0) {
      const successRate = ((results.created / results.totalProcessed) * 100).toFixed(1);
      logger.info(`Success rate: ${successRate}%`);
    }

    if (results.errors > 0) {
      logger.info('\nRecent errors:');
      results.errorDetails.slice(-5).forEach((detail, index) => {
        logger.error(`${index + 1}. ${detail.item || detail.page}: ${detail.error}`);
      });
    }

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
  const processor = new OvernightMountingsAPIProcessor();

  try {
    // Configuration options
    const options = {
      startPage: 1,
      maxPages: null, // Process all pages
      batchSize: 5, // Smaller batches for API processing
      delayBetweenBatches: 2000, // 2 seconds between batches
      delayBetweenPages: 1000, // 1 second between API pages
      dryRun: false,
      enableFiltering: true
    };

    // Process command line arguments
    const args = process.argv.slice(2);
    
    if (args.includes('--dry-run')) {
      options.dryRun = true;
      logger.info('Running in DRY RUN mode');
    }

    if (args.includes('--no-filter')) {
      options.enableFiltering = false;
      logger.info('Filtering disabled - processing all products');
    }

    const startPageIndex = args.indexOf('--start-page');
    if (startPageIndex !== -1 && args[startPageIndex + 1]) {
      options.startPage = parseInt(args[startPageIndex + 1]);
    }

    const maxPagesIndex = args.indexOf('--max-pages');
    if (maxPagesIndex !== -1 && args[maxPagesIndex + 1]) {
      options.maxPages = parseInt(args[maxPagesIndex + 1]);
    }

    // Execute processing
    const results = await processor.processProductsFromAPI(options);

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
if (require.main === module) {
  main();
}

module.exports = OvernightMountingsAPIProcessor;