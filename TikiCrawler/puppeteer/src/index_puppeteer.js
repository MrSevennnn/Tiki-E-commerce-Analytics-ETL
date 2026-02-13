/**
 * Tiki Puppeteer Crawler - Entry Point (FALLBACK)
 * Vietnam E-commerce Analytics Platform
 * 
 * This is the FALLBACK entry point using Puppeteer when API crawler is blocked.
 * Uses network interception to capture API responses during browser automation.
 * 
 * Usage:
 *   npm run crawl:puppeteer     # Use this as fallback
 *   npm run crawl               # Primary API crawler
 */

import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { crawlCategoryViaPuppeteer, crawlMultipleCategoriesViaPuppeteer } from './puppeteer_crawler.js';
import { CATEGORY_URLS, MAX_PAGES, GCS_CONFIG } from './config.js';
import { uploadToGCS, buildGCSDestinationPath, deleteLocalFile } from './gcs.js';
import { logInfo, logSuccess, logError, getOutputDirectory, saveToJson } from './utils.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// =============================================================================
// MAIN FUNCTION
// =============================================================================

async function main() {
  console.log('\n' + '='.repeat(70));
  console.log('TIKI PUPPETEER CRAWLER (FALLBACK MODE)');
  console.log('Vietnam E-commerce Analytics Platform');
  console.log('='.repeat(70));
  console.log(`Started at: ${new Date().toISOString()}`);
  console.log(`Categories: ${CATEGORY_URLS.length}`);
  console.log(`Max Pages per Category: ${MAX_PAGES}`);
  console.log('='.repeat(70) + '\n');

  const startTime = Date.now();
  const allProducts = [];
  const generatedFiles = [];
  const outputDir = getOutputDirectory();

  // Ensure output directory exists
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  try {
    // Crawl each category
    for (let i = 0; i < CATEGORY_URLS.length; i++) {
      const categoryUrl = CATEGORY_URLS[i];
      const categoryName = categoryUrl.match(/\/([^/]+)\/c\d+/)?.[1] || `category_${i + 1}`;

      console.log(`\n[${ i + 1}/${CATEGORY_URLS.length}] Crawling: ${categoryName}`);

      try {
        const products = await crawlCategoryViaPuppeteer(categoryUrl, {
          maxPages: MAX_PAGES,
          enrichSeller: true,
        });

        if (products.length > 0) {
          const filename = `${categoryName}_puppeteer.json`;
          const filePath = path.join(outputDir, filename);
          saveToJson(filePath, products);
          generatedFiles.push(filePath);

          allProducts.push(...products);
          logSuccess(`Category ${categoryName}: ${products.length} products saved`);
        }

        // Delay between categories
        if (i < CATEGORY_URLS.length - 1) {
          logInfo('Waiting 5 seconds before next category...');
          await new Promise(resolve => setTimeout(resolve, 5000));
        }

      } catch (categoryError) {
        logError(`Category ${categoryName}`, categoryError);
        continue;
      }
    }

    // -------------------------------------------------------------------------
    // Upload to GCS
    // -------------------------------------------------------------------------
    if (GCS_CONFIG.enabled && generatedFiles.length > 0) {
      console.log('\n' + '-'.repeat(60));
      console.log('UPLOADING TO GOOGLE CLOUD STORAGE');
      console.log('-'.repeat(60));

      for (const filePath of generatedFiles) {
        try {
          const filename = path.basename(filePath);
          const destinationPath = buildGCSDestinationPath(filename);
          await uploadToGCS(filePath, destinationPath);
          logSuccess(`Uploaded: ${filename}`);

          // Delete local file after successful upload
          if (GCS_CONFIG.deleteAfterUpload) {
            deleteLocalFile(filePath);
          }
        } catch (uploadError) {
          logError(`Upload ${filePath}`, uploadError);
        }
      }
    }

    // -------------------------------------------------------------------------
    // Final Summary
    // -------------------------------------------------------------------------
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    const uniqueProducts = new Set(allProducts.map(p => p.product_id));
    const duplicateRate = allProducts.length > 0 
      ? ((allProducts.length - uniqueProducts.size) / allProducts.length * 100).toFixed(2)
      : 0;

    console.log('\n' + '='.repeat(70));
    console.log('PUPPETEER CRAWL COMPLETE');
    console.log('='.repeat(70));
    console.log(`Total Products: ${allProducts.length}`);
    console.log(`Unique Products: ${uniqueProducts.size}`);
    console.log(`Duplicate Rate: ${duplicateRate}%`);
    console.log(`Files Generated: ${generatedFiles.length}`);
    console.log(`Duration: ${duration} seconds`);
    console.log(`Finished at: ${new Date().toISOString()}`);
    console.log('='.repeat(70) + '\n');

    return {
      success: true,
      totalProducts: allProducts.length,
      uniqueProducts: uniqueProducts.size,
      duplicateRate: parseFloat(duplicateRate),
      filesGenerated: generatedFiles.length,
      duration: parseFloat(duration),
    };

  } catch (error) {
    logError('Puppeteer Crawler Main', error);
    throw error;
  }
}

// =============================================================================
// RUN
// =============================================================================

main()
  .then((result) => {
    process.exit(0);
  })
  .catch((error) => {
    console.error('Crawl failed:', error);
    process.exit(1);
  });
