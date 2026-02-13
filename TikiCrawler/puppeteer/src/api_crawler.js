/**
 * Tiki API-based Product Crawler
 * Uses Tiki's official API endpoint instead of DOM scraping
 * This fixes the pagination issue where URL-based pagination no longer works
 */

import fetch from 'node-fetch';
import fs from 'fs';
import path from 'path';
import { OUTPUT, MAX_PAGES } from './config.js';
import { randomSleep, extractCategoryName, logInfo, logSuccess, logError } from './utils.js';

/**
 * Extract category ID from Tiki category URL
 * Example: https://tiki.vn/dien-thoai-may-tinh-bang/c1789 → 1789
 */
function extractCategoryId(categoryUrl) {
  const match = categoryUrl.match(/\/c(\d+)/);
  return match ? match[1] : null;
}

/**
 * Extract URL key from Tiki category URL
 * Example: https://tiki.vn/dien-thoai-may-tinh-bang/c1789 → dien-thoai-may-tinh-bang
 */
function extractUrlKey(categoryUrl) {
  const match = categoryUrl.match(/tiki\.vn\/([^/]+)\/c\d+/);
  return match ? match[1] : null;
}

/**
 * Build Tiki API URL for listings
 */
function buildApiUrl(categoryId, urlKey, page, limit = 40) {
  const baseUrl = 'https://tiki.vn/api/personalish/v1/blocks/listings';
  const params = new URLSearchParams({
    limit: limit.toString(),
    include: 'advertisement',
    aggregations: '2',
    version: 'home-persionalized',
    trackity_id: generateTrackityId(),
    category: categoryId,
    page: page.toString(),
    urlKey: urlKey,
  });
  
  return `${baseUrl}?${params.toString()}`;
}

/**
 * Generate a random trackity_id (UUID-like format)
 * Tiki uses this for tracking but it doesn't affect data retrieval
 */
function generateTrackityId() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    const r = Math.random() * 16 | 0;
    const v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

/**
 * Common headers for Tiki API requests
 */
function getTikiHeaders() {
  return {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
    'Referer': 'https://tiki.vn/',
    'x-guest-token': '1',
  };
}

/**
 * Fetch category name from Category API
 * UPDATED: 2026-02-04 - Get accurate category name for dimension table
 */
async function fetchCategoryName(categoryId) {
  const url = `https://tiki.vn/api/v2/categories/${categoryId}`;
  
  try {
    const response = await fetch(url, {
      headers: getTikiHeaders(),
    });
    
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    
    const data = await response.json();
    return data.name || null;
  } catch (error) {
    // Silent fail - will keep NULL in database
    return null;
  }
}

/**
 * Fetch product details (seller info, breadcrumbs, specs, etc.)
 * UPDATED: 2026-02-04 - Extract breadcrumbs for category_name
 */
async function fetchProductDetails(productId) {
  const url = `https://tiki.vn/api/v2/products/${productId}`;
  
  try {
    const response = await fetch(url, {
      headers: getTikiHeaders(),
    });
    
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    
    const data = await response.json();
    
    // Extract leaf category name from breadcrumbs
    let categoryName = null;
    if (data.breadcrumbs && Array.isArray(data.breadcrumbs) && data.breadcrumbs.length > 0) {
      // Get last breadcrumb (leaf category)
      const leafBreadcrumb = data.breadcrumbs[data.breadcrumbs.length - 1];
      categoryName = leafBreadcrumb?.name || null;
    }
    
    return {
      seller_id: data.current_seller?.id || null,
      seller: data.current_seller?.name || null,
      seller_logo: data.current_seller?.logo || null,
      category_name: categoryName,
      warehouse_id: data.inventory?.warehouse_id || null,
    };
  } catch (error) {
    logError(`Failed to fetch details for product ${productId}: ${error.message}`);
    return {
      seller_id: null,
      seller: null,
      seller_logo: null,
      warehouse_id: null,
    };
  }
}

/**
 * Extract leaf category info from API product data
 * API returns: category_ids (array) and primary_category_path (string)
 * 
 * UPDATED: 2026-02-04 - Extract from primary_category_path if category_ids is empty
 */
function extractLeafCategory(product) {
  const categoryIds = product.category_ids || [];
  const primaryCategoryPath = product.primary_category_path || '';
  const primaryCategoryName = product.primary_category_name || null;
  
  let categoryId = null;
  let categoryName = null;
  let rootCategoryId = null;
  let categoryDepth = 0;
  let categoryPath = '';
  
  // Parse primary_category_path to array (format: "1/2/1789/1795")
  let pathIds = [];
  if (primaryCategoryPath) {
    pathIds = primaryCategoryPath
      .split('/')
      .filter(p => p && p !== '1' && p !== '2')
      .map(id => parseInt(id, 10));
    categoryPath = pathIds.join(' > ');
  }
  
  // Extract from category_ids array if available (priority)
  if (categoryIds.length > 0) {
    categoryId = categoryIds[categoryIds.length - 1]; // LEAF
    categoryName = null; // Will be filled from breadcrumbs in fetchProductDetails
    rootCategoryId = categoryIds[0]; // ROOT
    categoryDepth = categoryIds.length;
  }
  // Fallback: Extract from pathIds if category_ids is empty
  else if (pathIds.length > 0) {
    categoryId = pathIds[pathIds.length - 1]; // LEAF from path
    categoryName = null; // Will be filled from breadcrumbs in fetchProductDetails
    rootCategoryId = pathIds[0]; // ROOT from path
    categoryDepth = pathIds.length;
  }
  
  return {
    category_id: categoryId,
    category_name: categoryName,
    root_category_id: rootCategoryId,
    category_depth: categoryDepth,
    category_path: categoryPath || categoryName || '',
  };
}

/**
 * Fetch products from Tiki API
 */
async function fetchProductsFromApi(categoryId, urlKey, page) {
  const apiUrl = buildApiUrl(categoryId, urlKey, page);
  
  try {
    const response = await fetch(apiUrl, {
      headers: getTikiHeaders(),
    });
    
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    
    const data = await response.json();
    
    // Extract products from response
    const products = data.data || [];
    const paging = data.paging || {};
    
    return {
      products,
      paging,
      hasMore: paging.current_page < paging.last_page,
      currentPage: paging.current_page,
      totalPages: paging.last_page,
      totalItems: paging.total,
    };
  } catch (error) {
    logError(`Failed to fetch page ${page}: ${error.message}`);
    return {
      products: [],
      paging: {},
      hasMore: false,
      currentPage: page,
      totalPages: page,
      totalItems: 0,
    };
  }
}

/**
 * Crawl a single category using API
 */
export async function crawlCategoryViaApi(categoryUrl, maxPages = MAX_PAGES) {
  const categoryName = extractCategoryName(categoryUrl);
  const categoryId = extractCategoryId(categoryUrl);
  const urlKey = extractUrlKey(categoryUrl);
  
  if (!categoryId || !urlKey) {
    logError(`Invalid category URL: ${categoryUrl}`);
    return { totalProducts: 0, filesGenerated: 0 };
  }
  
  logInfo(`\n${'='.repeat(60)}`);
  logInfo(`CRAWLING CATEGORY (API MODE): ${categoryName}`);
  logInfo(`Category ID: ${categoryId} | URL Key: ${urlKey}`);
  logInfo(`${'='.repeat(60)}\n`);
  
  const outputDir = path.join(OUTPUT.baseDir, OUTPUT.dateFormat());
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }
  
  let totalProducts = 0;
  let filesGenerated = 0;
  let currentPage = 1;
  let hasMore = true;
  
  while (hasMore && currentPage <= maxPages) {
    logInfo(`Fetching page ${currentPage}...`);
    
    const result = await fetchProductsFromApi(categoryId, urlKey, currentPage);
    
    if (result.products.length === 0) {
      logInfo(`No products found on page ${currentPage}. Stopping.`);
      break;
    }
    
    // Transform products to match expected format
    const transformedProducts = result.products.map(product => {
      // Extract leaf category info
      const leafCategory = extractLeafCategory(product);
      
      return {
        product_id: product.id,
        sku: product.sku,
        name: product.name,
        url_key: product.url_key,
        product_url: product.url_path 
          ? `https://tiki.vn/${product.url_path}` 
          : `https://tiki.vn/${product.url_key}.html`,
        brand: product.brand_name || null,
        price: product.price,
        original_price: product.original_price || product.price,
        discount_rate: product.discount_rate || 0,
        rating: product.rating_average || 0,
        review_count: product.review_count || 0,
        quantity_sold: product.quantity_sold?.value || 0,
        thumbnail_url: product.thumbnail_url,
        seller: null, // Will be enriched later
        seller_id: null, // Will be enriched later
        seller_logo: null,
        warehouse_id: null,
        badges: product.badges_new?.map(b => b.code) || [],
        inventory_status: product.inventory_status || 'available',
        
        // =============================================================
        // LEAF CATEGORY FIELDS (NEW - for accurate trend mapping)
        // =============================================================
        category_id: leafCategory.category_id,           // LEAF category ID
        category_name: leafCategory.category_name,       // LEAF category name
        root_category_id: leafCategory.root_category_id, // ROOT category ID
        category_depth: leafCategory.category_depth,     // Hierarchy depth
        category_path: leafCategory.category_path,       // Path as IDs
        
        _extracted_at: new Date().toISOString(),
        _source_page: currentPage,
        _category_url: categoryUrl,
        _category_name: categoryName,
        _root_category_id: categoryId, // Original root for reference
      };
    });
    
    // Enrich with seller info (API v2 product details)
    logInfo(`  Enriching ${transformedProducts.length} products with seller info...`);
    for (let i = 0; i < transformedProducts.length; i++) {
      const product = transformedProducts[i];
      const details = await fetchProductDetails(product.product_id);
      
      product.seller_id = details.seller_id;
      product.seller = details.seller;
      product.seller_logo = details.seller_logo;
      product.warehouse_id = details.warehouse_id;
      
      // Rate limiting: 500ms per request
      if (i < transformedProducts.length - 1) {
        await new Promise(resolve => setTimeout(resolve, 500));
      }
      
      // Progress indicator every 10 products
      if ((i + 1) % 10 === 0 || i === transformedProducts.length - 1) {
        logInfo(`    ↳ Enriched ${i + 1}/${transformedProducts.length} products`);
      }
    }
    
    // Enrich category names from Category API (once per unique category_id)
    const uniqueCategoryIds = [...new Set(transformedProducts.map(p => p.category_id).filter(id => id))];
    if (uniqueCategoryIds.length > 0) {
      logInfo(`  Fetching category names for ${uniqueCategoryIds.length} unique categories...`);
      const categoryNames = new Map();
      
      for (const catId of uniqueCategoryIds) {
        const name = await fetchCategoryName(catId);
        if (name) {
          categoryNames.set(catId, name);
        }
        // Rate limiting
        await new Promise(resolve => setTimeout(resolve, 300));
      }
      
      // Apply category names to products
      for (const product of transformedProducts) {
        if (categoryNames.has(product.category_id)) {
          product.category_name = categoryNames.get(product.category_id);
        }
      }
      
      logInfo(`    ↳ Fetched ${categoryNames.size}/${uniqueCategoryIds.length} category names`);
    }
    
    // Save to file
    const filename = `${categoryName}_page_${currentPage}.json`;
    const filepath = path.join(outputDir, filename);
    
    fs.writeFileSync(filepath, JSON.stringify(transformedProducts, null, 2));
    
    totalProducts += transformedProducts.length;
    filesGenerated++;
    
    logSuccess(`✓ Page ${currentPage}: ${transformedProducts.length} products saved to ${filename}`);
    logInfo(`  Total pages: ${result.totalPages} | Has more: ${result.hasMore}`);
    
    hasMore = result.hasMore && currentPage < maxPages;
    currentPage++;
    
    if (hasMore) {
      // Random delay between pages to be polite
      const delay = Math.random() * 2000 + 1000; // 1-3 seconds
      logInfo(`  Waiting ${(delay / 1000).toFixed(1)}s before next page...\n`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  
  logSuccess(`\n${'='.repeat(60)}`);
  logSuccess(`CATEGORY COMPLETE: ${categoryName}`);
  logSuccess(`Total Products: ${totalProducts} | Files Generated: ${filesGenerated}`);
  logSuccess(`${'='.repeat(60)}\n`);
  
  return { totalProducts, filesGenerated };
}

/**
 * Crawl multiple categories
 */
export async function crawlMultipleCategoriesViaApi(categoryUrls, maxPages = MAX_PAGES) {
  const startTime = Date.now();
  let grandTotal = 0;
  let totalFiles = 0;
  
  logInfo(`\n${'*'.repeat(70)}`);
  logInfo(`STARTING MULTI-CATEGORY CRAWL (API MODE)`);
  logInfo(`Categories: ${categoryUrls.length} | Max Pages per Category: ${maxPages}`);
  logInfo(`${'*'.repeat(70)}\n`);
  
  for (let i = 0; i < categoryUrls.length; i++) {
    const categoryUrl = categoryUrls[i];
    const categoryName = extractCategoryName(categoryUrl);
    
    logInfo(`\n[${ i + 1}/${categoryUrls.length}] Processing: ${categoryName}`);
    
    const result = await crawlCategoryViaApi(categoryUrl, maxPages);
    
    grandTotal += result.totalProducts;
    totalFiles += result.filesGenerated;
    
    // Delay between categories
    if (i < categoryUrls.length - 1) {
      const delay = 3000;
      logInfo(`Waiting ${delay / 1000}s before next category...\n`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  
  const duration = ((Date.now() - startTime) / 1000).toFixed(2);
  
  logSuccess(`\n${'*'.repeat(70)}`);
  logSuccess(`ALL CATEGORIES CRAWL COMPLETED (API MODE)`);
  logSuccess(`Total Categories: ${categoryUrls.length}`);
  logSuccess(`Total Products: ${grandTotal}`);
  logSuccess(`Total Files: ${totalFiles}`);
  logSuccess(`Duration: ${duration} seconds`);
  logSuccess(`${'*'.repeat(70)}\n`);
  
  return { grandTotal, totalFiles, duration };
}
