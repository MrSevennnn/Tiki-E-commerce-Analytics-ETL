/**
 * 
 * This is the new crawler that uses Tiki's API directly instead of
 * scraping the DOM. This fixes the pagination issue where URL-based
 * pagination (?page=2) no longer works.
 * 
 * Usage:
 *   npm start
 *   npm run crawl
 * 
 */

import { CATEGORY_URLS, MAX_PAGES } from './config.js';
import { crawlMultipleCategoriesViaApi } from './api_crawler.js';
import { uploadToGCS, buildGCSDestinationPath, deleteLocalFile } from './gcs.js';
import { logInfo, logSuccess, logError } from './utils.js';
import fs from 'fs';
import path from 'path';
import { OUTPUT } from './config.js';

/**
 * Main execution function
 */
async function main() {
  const startTime = Date.now();
  
  try {
    logInfo('\n' + '═'.repeat(70));
    logInfo('TIKI API CRAWLER v2.0 - Starting...');
    logInfo('═'.repeat(70) + '\n');
    
    const result = await crawlMultipleCategoriesViaApi(CATEGORY_URLS, MAX_PAGES);
    
    const outputDir = path.join(OUTPUT.baseDir, OUTPUT.dateFormat());
    const files = fs.readdirSync(outputDir).filter(f => f.endsWith('.json'));
    
    logInfo(`\nUploading ${files.length} files to Google Cloud Storage...`);
    
    let uploadedCount = 0;
    let failedCount = 0;
    
    for (const filename of files) {
      const localPath = path.join(outputDir, filename);
      const gcsPath = buildGCSDestinationPath(filename);
      
      try {
        await uploadToGCS(localPath, gcsPath);
        uploadedCount++;
        
        // Optionally delete local file after successful upload
        // Uncomment the line below to enable auto-deletion
        // await deleteLocalFile(localPath);
      } catch (error) {
        logError(`Failed to upload ${filename}: ${error.message}`);
        failedCount++;
      }
    }
    
    const totalDuration = ((Date.now() - startTime) / 1000).toFixed(2);
    
    logSuccess('\n' + '═'.repeat(70));
    logSuccess('CRAWL COMPLETED SUCCESSFULLY!');
    logSuccess('═'.repeat(70));
    logInfo(`Total Products Crawled: ${result.grandTotal}`);
    logInfo(`Files Generated: ${result.totalFiles}`);
    logInfo(`Files Uploaded to GCS: ${uploadedCount}`);
    if (failedCount > 0) {
      logError(`Files Failed to Upload: ${failedCount}`);
    }
    logInfo(`Total Duration: ${totalDuration} seconds`);
    logSuccess('═'.repeat(70) + '\n');
    
    process.exit(0);
  } catch (error) {
    logError('\n' + '═'.repeat(70));
    logError('CRAWL FAILED!');
    logError('═'.repeat(70));
    logError(`Error: ${error.message}`);
    logError(error.stack);
    logError('═'.repeat(70) + '\n');
    
    process.exit(1);
  }
}

main();
