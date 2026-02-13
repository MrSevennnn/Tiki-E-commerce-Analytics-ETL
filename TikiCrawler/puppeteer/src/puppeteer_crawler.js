/**
 * Tiki Puppeteer Crawler - FALLBACK VERSION
 * Vietnam E-commerce Analytics Platform
 * 
 * This is a FALLBACK crawler using Puppeteer when API-based crawling is blocked.
 * Strategy: Intercept network requests to capture API responses during pagination.
 * 
 * Key approach:
 * 1. Navigate to category page
 * 2. Set up request interception to capture /api/personalish/v1/blocks/listings
 * 3. Click pagination buttons or scroll to trigger API calls
 * 4. Capture and parse API responses
 * 
 * Usage:
 *   import { crawlCategoryViaPuppeteer } from './puppeteer_crawler.js';
 */

import { initBrowser, createPage, closeBrowser } from './browser.js';
import { logInfo, logError, logSuccess } from './utils.js';

// =============================================================================
// CONFIGURATION
// =============================================================================

const API_PATTERN = /api\/personalish\/v1\/blocks\/listings/;
const PRODUCT_API_PATTERN = /api\/v2\/products\/\d+/;

// =============================================================================
// NETWORK INTERCEPTION CRAWLER
// =============================================================================

/**
 * Crawl a single category using Puppeteer with network interception
 * @param {string} categoryUrl - Category URL (e.g., https://tiki.vn/dien-thoai-may-tinh-bang/c1789)
 * @param {object} options - Crawl options
 * @returns {Promise<Array>} Array of products
 */
export async function crawlCategoryViaPuppeteer(categoryUrl, options = {}) {
  const {
    maxPages = 10,
    enrichSeller = true,
  } = options;

  let browser = null;
  let allProducts = [];
  const capturedResponses = [];
  
  logInfo(`\n${'='.repeat(60)}`);
  logInfo(`PUPPETEER FALLBACK CRAWLER`);
  logInfo(`Category: ${categoryUrl}`);
  logInfo(`Max Pages: ${maxPages}`);
  logInfo(`${'='.repeat(60)}\n`);

  try {
    browser = await initBrowser();
    const page = await createPage(browser);

    // -------------------------------------------------------------------------
    // Setup Network Interception
    // -------------------------------------------------------------------------
    await page.setRequestInterception(true);
    
    // Track captured API responses
    const apiResponses = new Map();
    
    page.on('request', (request) => {
      // Allow all requests to proceed
      request.continue();
    });

    page.on('response', async (response) => {
      const url = response.url();
      
      // Capture listing API responses
      if (API_PATTERN.test(url)) {
        try {
          const json = await response.json();
          if (json?.data) {
            const pageMatch = url.match(/page=(\d+)/);
            const pageNum = pageMatch ? parseInt(pageMatch[1]) : 1;
            apiResponses.set(pageNum, json.data);
            logInfo(`[INTERCEPT] Captured page ${pageNum} with ${json.data.length} products`);
          }
        } catch (e) {
          // Response might not be JSON
        }
      }
    });

    logInfo(`Navigating to: ${categoryUrl}`);
    await page.goto(categoryUrl, {
      waitUntil: 'networkidle2',
      timeout: 60000,
    });

    await page.waitForSelector('.product-item, [data-view-id*="product"]', {
      timeout: 30000,
    }).catch(() => {
      logInfo('Product selector not found, trying alternative...');
    });

    // Allow initial API call to complete
    await new Promise(resolve => setTimeout(resolve, 2000));

    // -------------------------------------------------------------------------
    // Extract Page 1 Data (from initial load)
    // -------------------------------------------------------------------------
    let page1Products = await extractProductsFromPage(page);
    
    // If no products from DOM, try from captured API
    if (page1Products.length === 0 && apiResponses.has(1)) {
      page1Products = transformApiProducts(apiResponses.get(1), categoryUrl, 1);
    }
    
    if (page1Products.length > 0) {
      logInfo(`Page 1: Extracted ${page1Products.length} products`);
      allProducts.push(...page1Products);
    }

    // -------------------------------------------------------------------------
    // Crawl Additional Pages via Pagination Clicks
    // -------------------------------------------------------------------------
    for (let pageNum = 2; pageNum <= maxPages; pageNum++) {
      try {
        logInfo(`\n--- Processing Page ${pageNum} ---`);
        
        // Clear previous page data from map for this page
        apiResponses.delete(pageNum);
        
        // Method 1: Click pagination button
        const paginationClicked = await clickPaginationButton(page, pageNum);
        
        if (!paginationClicked) {
          // Method 2: Navigate via URL with page parameter
          const pageUrl = `${categoryUrl}?page=${pageNum}`;
          logInfo(`Navigating to: ${pageUrl}`);
          await page.goto(pageUrl, {
            waitUntil: 'networkidle2',
            timeout: 30000,
          });
        }

        // Wait for API response to be captured
        await new Promise(resolve => setTimeout(resolve, 3000));

        // Scroll to trigger lazy loading
        await autoScroll(page);
        await new Promise(resolve => setTimeout(resolve, 1000));

        // Try to get products from captured API response first
        let pageProducts = [];
        
        if (apiResponses.has(pageNum)) {
          pageProducts = transformApiProducts(apiResponses.get(pageNum), categoryUrl, pageNum);
          logInfo(`[API] Got ${pageProducts.length} products from intercepted API`);
        } else {
          // Fallback: Extract from DOM
          pageProducts = await extractProductsFromPage(page, pageNum);
          logInfo(`[DOM] Got ${pageProducts.length} products from page DOM`);
        }

        if (pageProducts.length === 0) {
          logInfo(`No products found on page ${pageNum}, stopping pagination`);
          break;
        }

        // Check for duplicates with previous pages
        const existingIds = new Set(allProducts.map(p => p.product_id));
        const newProducts = pageProducts.filter(p => !existingIds.has(p.product_id));
        
        if (newProducts.length === 0) {
          logInfo(`All products on page ${pageNum} are duplicates, stopping`);
          break;
        }

        allProducts.push(...newProducts);
        logInfo(`Page ${pageNum}: Added ${newProducts.length} new products (${pageProducts.length - newProducts.length} duplicates skipped)`);

        // Rate limiting
        await new Promise(resolve => setTimeout(resolve, 1500));

      } catch (pageError) {
        logError(`Page ${pageNum}`, pageError);
        continue;
      }
    }

    // -------------------------------------------------------------------------
    // Enrich with Seller Info (Optional)
    // -------------------------------------------------------------------------
    if (enrichSeller && allProducts.length > 0) {
      logInfo(`\nEnriching ${allProducts.length} products with seller info...`);
      allProducts = await enrichProductsWithSeller(page, allProducts);
    }

    await page.close();

  } catch (error) {
    logError('Puppeteer Crawler', error);
    throw error;
  } finally {
    if (browser) {
      await closeBrowser(browser);
    }
  }

  // Final stats
  logSuccess(`\nPuppeteer Crawl Complete!`);
  logInfo(`Total Products: ${allProducts.length}`);
  
  const uniqueIds = new Set(allProducts.map(p => p.product_id));
  const duplicateRate = ((allProducts.length - uniqueIds.size) / allProducts.length * 100).toFixed(2);
  logInfo(`Unique Products: ${uniqueIds.size}`);
  logInfo(`Duplicate Rate: ${duplicateRate}%`);

  return allProducts;
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Click pagination button for specific page
 */
async function clickPaginationButton(page, pageNum) {
  try {
    // Try different pagination selectors
    const selectors = [
      `a[href*="page=${pageNum}"]`,
      `button[data-page="${pageNum}"]`,
      `.pagination a:contains("${pageNum}")`,
      `[data-view-id*="pagination"] a`,
    ];

    for (const selector of selectors) {
      try {
        const elements = await page.$$(selector);
        for (const el of elements) {
          const text = await page.evaluate(e => e.textContent?.trim(), el);
          if (text === String(pageNum)) {
            await el.click();
            await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 10000 }).catch(() => {});
            return true;
          }
        }
      } catch (e) {
        continue;
      }
    }

    return false;
  } catch (error) {
    return false;
  }
}

/**
 * Auto-scroll to trigger lazy loading
 */
async function autoScroll(page) {
  await page.evaluate(async () => {
    await new Promise((resolve) => {
      let totalHeight = 0;
      const distance = 300;
      const timer = setInterval(() => {
        const scrollHeight = document.body.scrollHeight;
        window.scrollBy(0, distance);
        totalHeight += distance;
        
        if (totalHeight >= scrollHeight) {
          clearInterval(timer);
          resolve();
        }
      }, 100);
      
      // Max scroll time: 5 seconds
      setTimeout(() => {
        clearInterval(timer);
        resolve();
      }, 5000);
    });
  });
}

/**
 * Extract products from page DOM using multiple strategies
 */
async function extractProductsFromPage(page, pageNum = 1) {
  const products = await page.evaluate((currentPage) => {
    const results = [];
    
    // Strategy 1: Try __NEXT_DATA__
    try {
      const nextDataScript = document.getElementById('__NEXT_DATA__');
      if (nextDataScript) {
        const nextData = JSON.parse(nextDataScript.textContent);
        
        // Try multiple paths
        const paths = [
          nextData?.props?.initialState?.catalog?.data,
          nextData?.props?.pageProps?.data?.data,
          nextData?.props?.pageProps?.products,
        ];
        
        for (const productList of paths) {
          if (Array.isArray(productList) && productList.length > 0) {
            for (const p of productList) {
              results.push({
                product_id: p.id,
                sku: p.sku || p.seller_product_id?.toString(),
                name: p.name,
                url_key: p.url_key,
                brand: p.brand_name,
                price: p.price,
                original_price: p.original_price || p.list_price,
                discount_rate: p.discount_rate,
                rating: p.rating_average,
                review_count: p.review_count,
                quantity_sold: typeof p.quantity_sold === 'object' 
                  ? p.quantity_sold?.value 
                  : p.quantity_sold,
                thumbnail_url: p.thumbnail_url,
                seller: null,
                seller_id: p.seller_id,
                seller_logo: null,
                badges: p.badges?.map(b => b.code || b) || [],
                inventory_status: p.inventory_status || 'available',
                _source: 'next_data',
              });
            }
            break;
          }
        }
      }
    } catch (e) {
      console.log('NEXT_DATA extraction failed:', e.message);
    }

    // Strategy 2: DOM extraction if NEXT_DATA failed
    if (results.length === 0) {
      const cards = document.querySelectorAll('.product-item, [data-view-id*="product_listing"]');
      
      for (const card of cards) {
        try {
          // Get product ID from data attributes
          let productId = null;
          const viewContent = card.getAttribute('data-view-content');
          if (viewContent) {
            const parsed = JSON.parse(viewContent);
            productId = parsed?.click_data?.id;
          }
          
          if (!productId) {
            const href = card.getAttribute('href') || '';
            const match = href.match(/p(\d+)/);
            productId = match ? parseInt(match[1]) : null;
          }
          
          if (!productId) continue;

          const nameEl = card.querySelector('h3, [class*="name"]');
          const name = nameEl?.textContent?.trim();

          results.push({
            product_id: productId,
            name: name,
            _source: 'dom',
          });
        } catch (e) {
          continue;
        }
      }
    }

    return results;
  }, pageNum);

  return products.map(p => ({
    ...p,
    _extracted_at: new Date().toISOString(),
    _source_page: pageNum,
  }));
}

/**
 * Transform API response products to our schema
 */
function transformApiProducts(apiProducts, categoryUrl, pageNum) {
  if (!Array.isArray(apiProducts)) return [];
  
  // Extract category info from URL
  const urlMatch = categoryUrl.match(/\/([^/]+)\/c(\d+)/);
  const categoryName = urlMatch ? urlMatch[1] : 'unknown';
  const categoryId = urlMatch ? urlMatch[2] : null;

  return apiProducts.map(p => ({
    product_id: p.id,
    sku: p.sku || p.seller_product_id?.toString(),
    name: p.name,
    url_key: p.url_key,
    product_url: p.url_path ? `https://tiki.vn/${p.url_path}` : `https://tiki.vn/product-p${p.id}.html`,
    brand: p.brand_name,
    price: p.price,
    original_price: p.original_price || p.list_price,
    discount_rate: p.discount_rate,
    rating: p.rating_average,
    review_count: p.review_count,
    quantity_sold: typeof p.quantity_sold === 'object' ? p.quantity_sold?.value : p.quantity_sold,
    thumbnail_url: p.thumbnail_url,
    seller: null,  // Will be enriched later
    seller_id: p.seller_id,
    seller_logo: null,
    warehouse_id: null,
    badges: p.badges?.map(b => b.code || b) || [],
    inventory_status: p.inventory_status || 'available',
    _extracted_at: new Date().toISOString(),
    _source_page: pageNum,
    _category_url: categoryUrl,
    _category_name: categoryName,
    _category_id: categoryId,
  }));
}

/**
 * Enrich products with seller info by visiting product detail pages
 */
async function enrichProductsWithSeller(page, products) {
  const enriched = [];
  let enrichedCount = 0;

  for (let i = 0; i < products.length; i++) {
    const product = products[i];
    
    try {
      // Navigate to product page to get seller info
      const productUrl = product.product_url || `https://tiki.vn/product-p${product.product_id}.html`;
      
      // Use fetch API via page context to avoid full navigation
      const sellerInfo = await page.evaluate(async (productId) => {
        try {
          const response = await fetch(`https://tiki.vn/api/v2/products/${productId}`, {
            headers: {
              'Accept': 'application/json',
            },
          });
          
          if (!response.ok) return null;
          
          const data = await response.json();
          return {
            seller: data.current_seller?.name,
            seller_id: data.current_seller?.id,
            seller_logo: data.current_seller?.logo,
          };
        } catch (e) {
          return null;
        }
      }, product.product_id);

      if (sellerInfo) {
        product.seller = sellerInfo.seller;
        product.seller_id = sellerInfo.seller_id || product.seller_id;
        product.seller_logo = sellerInfo.seller_logo;
        enrichedCount++;
      }

      // Progress log every 10 products
      if ((i + 1) % 10 === 0) {
        logInfo(`Seller enrichment: ${i + 1}/${products.length} (${enrichedCount} enriched)`);
      }

      // Rate limiting
      await new Promise(resolve => setTimeout(resolve, 300));

    } catch (e) {
      // Keep product without seller info
    }

    enriched.push(product);
  }

  logInfo(`Seller enrichment complete: ${enrichedCount}/${products.length} products enriched`);
  return enriched;
}

// =============================================================================
// MULTI-CATEGORY CRAWLER
// =============================================================================

/**
 * Crawl multiple categories using Puppeteer
 */
export async function crawlMultipleCategoriesViaPuppeteer(categories, options = {}) {
  const allProducts = [];
  const stats = [];

  for (const categoryUrl of categories) {
    try {
      const products = await crawlCategoryViaPuppeteer(categoryUrl, options);
      allProducts.push(...products);
      
      stats.push({
        category: categoryUrl,
        count: products.length,
        status: 'success',
      });

      // Delay between categories
      await new Promise(resolve => setTimeout(resolve, 5000));

    } catch (error) {
      stats.push({
        category: categoryUrl,
        count: 0,
        status: 'failed',
        error: error.message,
      });
    }
  }

  console.log('\n' + '='.repeat(60));
  console.log('PUPPETEER CRAWL SUMMARY');
  console.log('='.repeat(60));
  
  for (const stat of stats) {
    console.log(`${stat.status === 'success' ? 'OK' : 'FAIL'} ${stat.category}: ${stat.count} products`);
  }
  
  console.log('='.repeat(60));
  console.log(`Total: ${allProducts.length} products`);

  return allProducts;
}

export default {
  crawlCategoryViaPuppeteer,
  crawlMultipleCategoriesViaPuppeteer,
};
