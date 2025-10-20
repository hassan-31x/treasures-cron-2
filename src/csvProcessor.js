const fs = require('fs');
const csv = require('csv-parser');
const logger = require('./logger');

class CSVProcessor {
  constructor() {
    this.totalRows = 0;
    this.processedRows = 0;
    this.errors = 0;
  }

  async processFile(filePath, options = {}) {
    return new Promise((resolve, reject) => {
      const results = [];
      const startTime = Date.now();
      
      logger.info(`Starting CSV processing: ${filePath}`);
      
      // Reset counters
      this.totalRows = 0;
      this.processedRows = 0;
      this.errors = 0;

      const stream = fs.createReadStream(filePath)
        .pipe(csv({
          skipEmptyLines: true,
          ...options
        }));

      stream.on('data', (row) => {
        this.totalRows++;
        
        try {
          // Process each row here
          const processedRow = this.processRow(row);
          
          if (processedRow) {
            results.push(processedRow);
            this.processedRows++;
          }

          // Log progress for large files
          if (this.totalRows % 10000 === 0) {
            logger.info(`Processed ${this.totalRows} rows...`);
          }

        } catch (error) {
          this.errors++;
          logger.warn(`Error processing row ${this.totalRows}:`, error.message);
        }
      });

      stream.on('end', () => {
        const endTime = Date.now();
        const duration = ((endTime - startTime) / 1000).toFixed(2);
        
        logger.info(`CSV processing completed in ${duration}s`);
        logger.info(`Total rows: ${this.totalRows}`);
        logger.info(`Successfully processed: ${this.processedRows}`);
        logger.info(`Errors: ${this.errors}`);

        // Log the first item as requested
        if (results.length > 0) {
          logger.info('First processed item:', JSON.stringify(results[0], null, 2));
        }

        resolve({
          data: results,
          stats: {
            totalRows: this.totalRows,
            processedRows: this.processedRows,
            errors: this.errors,
            duration: duration
          }
        });
      });

      stream.on('error', (error) => {
        logger.error('CSV processing error:', error.message);
        reject(error);
      });
    });
  }

  processRow(row) {
    // Basic row processing - customize based on your CSV structure
    try {
      // Remove empty fields and trim whitespace
      const cleanedRow = {};
      
      for (const [key, value] of Object.entries(row)) {
        const cleanKey = key.trim();
        const cleanValue = typeof value === 'string' ? value.trim() : value;
        
        if (cleanValue !== '' && cleanValue !== null && cleanValue !== undefined) {
          cleanedRow[cleanKey] = cleanValue;
        }
      }

      // Only return rows that have meaningful data
      if (Object.keys(cleanedRow).length > 0) {
        return cleanedRow;
      }

      return null;
    } catch (error) {
      logger.warn('Error processing individual row:', error.message);
      return null;
    }
  }

  async getFileInfo(filePath) {
    try {
      const stats = fs.statSync(filePath);
      const fileSizeMB = (stats.size / (1024 * 1024)).toFixed(2);
      
      // Estimate number of rows by reading first few lines
      const sampleSize = await this.estimateRowCount(filePath);
      
      return {
        size: stats.size,
        sizeMB: fileSizeMB,
        estimatedRows: sampleSize,
        lastModified: stats.mtime
      };
    } catch (error) {
      logger.error('Error getting file info:', error.message);
      throw error;
    }
  }

  async estimateRowCount(filePath, sampleLines = 100) {
    return new Promise((resolve, reject) => {
      let lineCount = 0;
      let byteCount = 0;
      const maxBytes = 1024 * 1024; // 1MB sample
      
      const stream = fs.createReadStream(filePath, { encoding: 'utf8' });
      
      stream.on('data', (chunk) => {
        byteCount += Buffer.byteLength(chunk);
        lineCount += (chunk.match(/\n/g) || []).length;
        
        if (byteCount >= maxBytes || lineCount >= sampleLines) {
          stream.destroy();
        }
      });

      stream.on('close', () => {
        if (lineCount > 0 && byteCount > 0) {
          const stats = fs.statSync(filePath);
          const estimatedLines = Math.round((stats.size / byteCount) * lineCount);
          resolve(estimatedLines);
        } else {
          resolve(0);
        }
      });

      stream.on('error', reject);
    });
  }
}

module.exports = CSVProcessor;
