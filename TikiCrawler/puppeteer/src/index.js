/**
 * Tiki Product Crawler - Main Entry Point
 * Vietnam E-commerce Analytics Platform - Phase 1: Ingestion
 * 
 * This script crawls product data from Tiki.vn category pages
 * and saves the raw data as JSON files for later processing.
 * After crawling, files are automatically uploaded to Google Cloud Storage.
 * 
 * Usage:
 *   npm run crawl
 *   DEBUG=true npm run crawl:debug
 * 
 * Output:
 *   Local: ./data/raw/tiki/{YYYYMMDD}/category_name_page_{N}.json
 *   GCS:   gs://BUCKET/raw_zone/tiki/products/snapshot_date=YYYY-MM-DD/filename.json
 */

import path from 'path';
import fs from 'fs';
import {
  CATEGORY_URL,
  CATEGORY_URLS,
  MAX_PAGES,
  TIMING,
  SELECTORS,
  GCS_CONFIG,
} from './config.js';
import {
  initBrowser,
  createPage,
  scrollToBottom,
  navigateToPage,
  closeBrowser,
} from './browser.js';
import {
  extractProducts,
  getTotalPages,
} from './extractor.js';
import {
  randomSleep,
  getOutputDirectory,
  saveToJson,
  generateFilename,
  extractCategoryName,
  buildPageUrl,
  logProgress,
  logError,
  logSuccess,
  logInfo,
} from './utils.js';
import {
  uploadToGCS,
  buildGCSDestinationPath,
  deleteLocalFile,
} from './gcs.js';

// =============================================================================
// MAIN CRAWLER FUNCTION
// =============================================================================

/**
 * Main crawler function for a single category
 * @param {string} categoryUrl - Category URL to crawl
 * @param {object} browser - Puppeteer browser instance
 * @returns {Promise<object>} Stats for this category
 */
async function crawlSingleCategory(categoryUrl, browser) {
  let totalProductsCrawled = 0;
  const categoryName = extractCategoryName(categoryUrl);
  const outputDir = getOutputDirectory();
  const generatedFiles = [];
  
  logInfo(`\n${'='.repeat(60)}`);
  logInfo(`CRAWLING CATEGORY: ${categoryName}`);
  logInfo(`URL: ${categoryUrl}`);
  logInfo(`${'='.repeat(60)}\n`);
  
  try {
    const page = await createPage(browser);
    
    const firstPageSuccess = await navigateToPage(page, categoryUrl);
    if (!firstPageSuccess) {
      throw new Error(`Failed to load first page for ${categoryName}`);
    }
    
    // Scroll to load all lazy-loaded content on first page
    await scrollToBottom(page);
    
    // Tiki uses URL-based pagination (?page=N)
    // We'll crawl up to MAX_PAGES, stopping when no products found
    const pagesToCrawl = MAX_PAGES;
    
    logInfo(`\nWill crawl up to ${pagesToCrawl} pages (using URL pagination ?page=N)\n`);
    
    // Crawl each page
    for (let currentPage = 1; currentPage <= pagesToCrawl; currentPage++) {
      try {
        console.log('\n' + '-'.repeat(40));
        logInfo(`Processing page ${currentPage}/${pagesToCrawl}...`);
        
        // Navigate to page (skip for first page as we're already there)
        if (currentPage > 1) {
          const pageUrl = buildPageUrl(categoryUrl, currentPage);
          const navSuccess = await navigateToPage(page, pageUrl);
          
          if (!navSuccess) {
            logError(`Page ${currentPage}`, new Error('Navigation failed, skipping...'));
            continue;
          }
          
          // Scroll to load lazy content
          await scrollToBottom(page);
        }
        
        const products = await extractProducts(page);
        
        if (products.length === 0) {
          logInfo(`No products found on page ${currentPage}, reached end of results`);
          break; // Stop crawling, no more pages
        }
        
        // Add metadata to products
        const enrichedProducts = products.map(product => ({
          ...product,
          _source_page: currentPage,
          _category_url: categoryUrl,
          _category_name: categoryName,
        }));
        
        // Save to JSON file
        const filename = generateFilename(categoryName, currentPage);
        const filePath = path.join(outputDir, filename);
        saveToJson(filePath, enrichedProducts);
        
        // Track file for GCS upload
        generatedFiles.push(filePath);
        
        // Update stats
        totalProductsCrawled += products.length;
        logProgress(currentPage, pagesToCrawl, products.length);
        
        // Add delay before next page (human-like behavior)
        if (currentPage < pagesToCrawl) {
          logInfo('Waiting before next page...');
          await randomSleep(TIMING.pageNavigationDelay);
        }
        
      } catch (pageError) {
        logError(`Page ${currentPage}`, pageError);
        // Continue to next page instead of crashing
        continue;
      }
    }
    
    await page.close();
    
    return {
      categoryName,
      totalProducts: totalProductsCrawled,
      filesGenerated: generatedFiles,
    };
    
  } catch (error) {
    logError(`Category ${categoryName}`, error);
    throw error;
  }
}

/**
 * Main crawler function - orchestrates crawling all categories
 */
async function crawlTikiCategories() {
  console.log('\n' + '='.repeat(60));
  console.log('TIKI PRODUCT CRAWLER - MULTI-CATEGORY');
  console.log('Vietnam E-commerce Analytics Platform - Phase 1: Ingestion');
  console.log('='.repeat(60) + '\n');
  
  logInfo(`Categories to crawl: ${CATEGORY_URLS.length}`);
  logInfo(`Max Pages per category: ${MAX_PAGES === Infinity ? 'All' : MAX_PAGES}\n`);
  
  let browser = null;
  const crawlStartTime = Date.now();
  const allGeneratedFiles = [];
  let totalProductsAllCategories = 0;
  
  try {
    // Initialize browser once for all categories
    browser = await initBrowser();
    
    // Crawl each category
    for (let i = 0; i < CATEGORY_URLS.length; i++) {
      const categoryUrl = CATEGORY_URLS[i];
      
      try {
        const result = await crawlSingleCategory(categoryUrl, browser);
        
        totalProductsAllCategories += result.totalProducts;
        allGeneratedFiles.push(...result.filesGenerated);
        
        logSuccess(`✓ Completed ${result.categoryName}: ${result.totalProducts} products`);
        
        // Wait between categories to avoid rate limiting
        if (i < CATEGORY_URLS.length - 1) {
          const delaySeconds = 3;
          logInfo(`Waiting ${delaySeconds}s before next category...`);
          await new Promise(resolve => setTimeout(resolve, delaySeconds * 1000));
        }
        
      } catch (categoryError) {
        logError(`Failed to crawl ${extractCategoryName(categoryUrl)}`, categoryError);
        // Continue with next category instead of stopping
      }
    }
    
    const crawlDuration = ((Date.now() - crawlStartTime) / 1000).toFixed(2);
    
    console.log('\n' + '='.repeat(60));
    logSuccess('ALL CATEGORIES CRAWL COMPLETED');
    console.log('='.repeat(60));
    console.log(`Total Categories Crawled: ${CATEGORY_URLS.length}`);
    console.log(`Total Products Crawled: ${totalProductsAllCategories}`);
    console.log(`Files Generated: ${allGeneratedFiles.length}`);
    console.log(`Duration: ${crawlDuration} seconds`);
    console.log('='.repeat(60) + '\n');
    
    // =========================================================================
    // GCS UPLOAD PHASE
    // =========================================================================
    
    const generatedFiles = allGeneratedFiles;
    
    if (generatedFiles.length > 0) {
      console.log('\n' + '='.repeat(60));
      console.log('UPLOADING TO GOOGLE CLOUD STORAGE');
      console.log('='.repeat(60) + '\n');
      
      logInfo(`Bucket: ${GCS_CONFIG.bucketName}`);
      logInfo(`Files to upload: ${generatedFiles.length}`);
      
      let uploadSuccess = true;
      let uploadedCount = 0;
      let failedCount = 0;
      
      for (const localFilePath of generatedFiles) {
        try {
          const filename = path.basename(localFilePath);
          const destinationPath = buildGCSDestinationPath(filename);
          
          const result = await uploadToGCS(localFilePath, destinationPath);
          
          if (result.success) {
            uploadedCount++;
            
            // Delete local file after successful upload
            if (GCS_CONFIG.deleteLocalAfterUpload) {
              deleteLocalFile(localFilePath);
            }
          }
          
        } catch (uploadError) {
          uploadSuccess = false;
          failedCount++;
          logError(`Failed to upload ${path.basename(localFilePath)}`, uploadError);
          // DO NOT delete local file on failure - allows manual retry
          logInfo(`  Keeping local file for manual retry: ${localFilePath}`);
        }
      }
      
      console.log('\n' + '='.repeat(60));
      if (uploadSuccess) {
        logSuccess('GCS UPLOAD COMPLETED');
        console.log(`Uploaded: ${uploadedCount}/${generatedFiles.length} files`);
      } else {
        logError('GCS UPLOAD PARTIALLY FAILED', new Error(`${failedCount} files failed`));
        console.log(`Uploaded: ${uploadedCount}/${generatedFiles.length} files`);
        console.log(`Failed: ${failedCount} files`);
        console.log('='.repeat(60) + '\n');
        
        // Exit with error code for Airflow to mark task as failed
        logError('Exiting with error code 1 for Airflow', new Error('GCS upload failed'));
        process.exit(1);
      }
      console.log('='.repeat(60) + '\n');
      
      // Clean up empty directory if all files uploaded
      if (uploadSuccess && GCS_CONFIG.deleteLocalAfterUpload) {
        try {
          const remainingFiles = fs.readdirSync(outputDir);
          if (remainingFiles.length === 0) {
            fs.rmdirSync(outputDir);
            logInfo(`Removed empty directory: ${outputDir}`);
          }
        } catch (cleanupError) {
          // Non-fatal, just log
          logInfo(`Could not remove directory: ${outputDir}`);
        }
      }
    }
    
  } catch (error) {
    logError('Crawler', error);
    process.exit(1);
  } finally {
    // Clean up
    if (browser) {
      await closeBrowser(browser);
    }
  }
}

// =============================================================================
// SCRIPT EXECUTION
// =============================================================================

crawlTikiCategories().catch(error => {
  logError('Unhandled Error', error);
  process.exit(1);
});
