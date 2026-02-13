/**
 * Data extraction module for Tiki product pages
 * Handles parsing of product cards and data cleaning
 * 
 * UPDATED: 2026-01-18 - Extract data from Tiki's JSON data embedded in page
 * Key findings:
 * - Tiki embeds product data as JSON in __NEXT_DATA__ script tag
 * - This contains rating_average, review_count, and all product info
 * - Much more reliable than parsing HTML elements
 */

import { SELECTORS } from './config.js';
import {
  cleanPrice,
  cleanDiscount,
  cleanRating,
  cleanReviewCount,
  logInfo,
  logError,
} from './utils.js';

// =============================================================================
// PRODUCT DATA EXTRACTION
// =============================================================================

/**
 * Extract all products from the current page
 * Uses Tiki's embedded JSON data for accurate extraction
 * @param {Page} page - Puppeteer page instance
 * @returns {Promise<Array>} Array of product objects
 */
export async function extractProducts(page) {
  logInfo('Extracting product data from page...');
  
  try {
    // First, try to extract from __NEXT_DATA__ JSON (most accurate)
    const jsonProducts = await extractFromNextData(page);
    
    if (jsonProducts && jsonProducts.length > 0) {
      logInfo(`Extracted ${jsonProducts.length} products from JSON data`);
      return jsonProducts;
    }
    
    // Fallback: extract from HTML elements
    logInfo('JSON extraction failed, falling back to HTML parsing...');
    const htmlProducts = await extractFromHTML(page);
    logInfo(`Extracted ${htmlProducts.length} products from HTML`);
    return htmlProducts;
    
  } catch (error) {
    logError('extractProducts', error);
    return [];
  }
}

/**
 * Extract LEAF category from Tiki product data
 * Leaf = category from primary_category_name (deepest level in path)
 * Root = first ID in primary_category_path
 * 
 * @param {String} primaryCategoryPath - Category path like "1/2/1789/1795"
 * @param {String} primaryCategoryName - Leaf category name like "Điện thoại Smartphone"
 * @param {Array} categoryIds - Array of category IDs [1789, 1795]
 * @returns {Object} Category info with leaf and root
 */
function extractCategoryInfo(primaryCategoryPath, primaryCategoryName, categoryIds) {
  let categoryId = null;
  let categoryName = null;
  let rootCategoryId = null;
  let categoryDepth = 0;
  let categoryPath = '';

  // Extract from category_ids array (last one is leaf)
  if (categoryIds && Array.isArray(categoryIds) && categoryIds.length > 0) {
    // Leaf category = LAST item in category_ids
    categoryId = categoryIds[categoryIds.length - 1];
    categoryName = primaryCategoryName || null;
    
    // Root category = FIRST item in category_ids
    rootCategoryId = categoryIds[0];
    
    categoryDepth = categoryIds.length;
  }
  
  // Build path from primary_category_path (format: "1/2/1789/1795")
  if (primaryCategoryPath) {
    const pathParts = primaryCategoryPath.split('/').filter(p => p && p !== '1' && p !== '2');
    categoryPath = pathParts.join(' > ');
    
    // If categoryId is null, try to extract from path
    if (categoryId === null && pathParts.length > 0) {
      categoryId = parseInt(pathParts[pathParts.length - 1], 10);
      rootCategoryId = parseInt(pathParts[0], 10);
      categoryDepth = pathParts.length;
    }
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
 * Extract products from __NEXT_DATA__ JSON script tag
 * This contains complete product data including rating, reviews, and breadcrumbs
 * 
 * UPDATED: 2026-02-02 - Added LEAF category extraction from breadcrumbs
 * 
 * @param {Page} page - Puppeteer page instance
 * @returns {Promise<Array>} Array of product objects
 */
async function extractFromNextData(page) {
  const products = await page.evaluate(() => {
    const results = [];
    
    try {
      // Find __NEXT_DATA__ script tag
      const nextDataScript = document.getElementById('__NEXT_DATA__');
      if (!nextDataScript) {
        console.log('__NEXT_DATA__ not found');
        return results;
      }
      
      const nextData = JSON.parse(nextDataScript.textContent);
      
      // Navigate to product list in the JSON structure
      // Correct path: props.initialState.catalog.data (NOT pageProps!)
      let productList = null;
      
      // Try different paths where products might be stored
      const paths = [
        nextData?.props?.initialState?.catalog?.data,      // Main path for category pages
        nextData?.props?.initialState?.catalog?.products,  // Alternative path
        nextData?.props?.pageProps?.initialState?.catalog?.data,
        nextData?.props?.pageProps?.products,
        nextData?.props?.pageProps?.data?.data,
      ];
      
      for (const path of paths) {
        if (path && Array.isArray(path) && path.length > 0) {
          productList = path;
          console.log('Found products at path with', path.length, 'items');
          break;
        }
      }
      
      if (!productList) {
        console.log('Product list not found in __NEXT_DATA__');
        console.log('Available keys in props:', Object.keys(nextData?.props || {}));
        return results;
      }
      
      // Extract data from each product
      for (const product of productList) {
        try {
          const extracted = {
            product_id: product.id?.toString() || product.master_id?.toString() || null,
            spid: product.seller_product_id?.toString() || product.spid?.toString() || null,
            name: product.name || null,
            price: product.price || null,
            original_price: product.original_price || product.list_price || null,
            discount_rate: product.discount_rate ? `-${product.discount_rate}%` : null,
            sales_volume: product.quantity_sold?.text || 
                         (product.quantity_sold?.value ? `Đã bán ${product.quantity_sold.value}` : null) ||
                         (typeof product.quantity_sold === 'number' ? `Đã bán ${product.quantity_sold}` : null),
            rating_average: product.rating_average || null,
            review_count: product.review_count || null,
            tiki_now: product.is_tikinow === true || product.is_tikinow === 1,
            product_url: product.url_path 
              ? `https://tiki.vn/${product.url_path}` 
              : `https://tiki.vn/product-p${product.id}.html`,
            image_url: product.thumbnail_url || null,
            // Additional useful fields
            brand_name: product.brand_name || null,
            seller_name: product.seller_name || null,
            seller_id: product.seller_id || null,
            
            // =============================================================
            // CATEGORY EXTRACTION (NEW - 2026-02-02)
            // Extract category data for leaf category parsing
            // =============================================================
            category_ids: product.category_ids || null,
            primary_category_path: product.primary_category_path || null,
            primary_category_name: product.primary_category_name || null,
          };
          
          results.push(extracted);
        } catch (e) {
          console.error('Error extracting product from JSON:', e.message);
          continue;
        }
      }
      
    } catch (error) {
      console.error('Error parsing __NEXT_DATA__:', error.message);
    }
    
    return results;
  });
  
  // Clean and format with LEAF category extraction
  return products.map(product => {
    // Extract leaf category from Tiki's category data
    const categoryInfo = extractCategoryInfo(
      product.primary_category_path,
      product.primary_category_name,
      product.category_ids
    );
    
    return {
      product_id: product.product_id,
      seller_product_id: product.spid,
      name: product.name,
      price: product.price,
      original_price: product.original_price,
      discount_rate: product.discount_rate,
      sales_volume: product.sales_volume,
      rating_average: product.rating_average,
      review_count: product.review_count,
      tiki_now: product.tiki_now,
      product_url: product.product_url,
      image_url: product.image_url,
      brand_name: product.brand_name,
      seller_name: product.seller_name,
      seller_id: product.seller_id,
      
      // =============================================================
      // LEAF CATEGORY FIELDS (NEW - for accurate trend mapping)
      // =============================================================
      category_id: categoryInfo.category_id,           // LEAF category ID (deepest level)
      category_name: categoryInfo.category_name,       // LEAF category name
      root_category_id: categoryInfo.root_category_id, // ROOT category ID (Level 1)
      category_depth: categoryInfo.category_depth,     // Hierarchy depth
      category_path: categoryInfo.category_path,       // Path as IDs: "1789 > 1795y)
      category_path: categoryInfo.category_path,       // Full path: "Root > Mid > Leaf"
      
      _extracted_at: new Date().toISOString(),
    };
  });
}

/**
 * Fallback: Extract products from HTML elements
 * @param {Page} page - Puppeteer page instance
 * @returns {Promise<Array>} Array of product objects
 */
async function extractFromHTML(page) {
  const products = await page.evaluate(() => {
    const results = [];
    const getText = (element) => element?.textContent?.trim() || null;
    
    const productCards = document.querySelectorAll('.product-item');
    
    for (const card of productCards) {
      try {
        // Extract product ID from data-view-content
        let productId = null;
        let spid = null;
        
        const viewContent = card.getAttribute('data-view-content');
        if (viewContent) {
          try {
            const parsed = JSON.parse(viewContent);
            productId = parsed?.click_data?.id?.toString() || null;
            spid = parsed?.click_data?.spid?.toString() || null;
          } catch (e) {
            const idMatch = viewContent.match(/"id":(\d+)/);
            const spidMatch = viewContent.match(/"spid":(\d+)/);
            productId = idMatch ? idMatch[1] : null;
            spid = spidMatch ? spidMatch[1] : null;
          }
        }
        
        if (!productId) continue;
        
        // Extract URL
        let href = card.getAttribute('href') || '';
        let productUrl = null;
        
        if (href.includes('tka.tiki.vn') || href.includes('pixel')) {
          productUrl = `https://tiki.vn/product-p${productId}.html${spid ? '?spid=' + spid : ''}`;
        } else if (href.startsWith('/')) {
          productUrl = `https://tiki.vn${href}`;
        } else if (href.startsWith('http')) {
          productUrl = href;
        } else {
          productUrl = `https://tiki.vn/product-p${productId}.html${spid ? '?spid=' + spid : ''}`;
        }
        
        // Extract name
        const nameEl = card.querySelector('h3');
        const name = getText(nameEl);
        
        // Extract price
        let priceRaw = null;
        const allElements = card.querySelectorAll('div, span');
        for (const el of allElements) {
          const text = el.textContent?.trim() || '';
          if ((text.includes('₫') || /^\d{1,3}(\.\d{3})+đ?$/.test(text.replace(/\s/g, ''))) && 
              !text.includes('Đã bán') && text.length < 30) {
            if (el.children.length === 0) {
              priceRaw = text;
              break;
            }
          }
        }
        
        // Extract discount
        let discountRaw = null;
        const discountEl = card.querySelector('[class*="badge-under-price"]') ||
                          card.querySelector('[class*="discount"]');
        if (discountEl) {
          const text = getText(discountEl);
          if (text && text.includes('%')) {
            discountRaw = text;
          }
        }
        
        // Extract sales volume
        let salesVolume = null;
        const quantityEl = card.querySelector('.quantity');
        if (quantityEl) {
          salesVolume = getText(quantityEl);
        }
        if (!salesVolume) {
          const cardText = card.textContent || '';
          const salesMatch = cardText.match(/Đã bán\s*([0-9,.kKmM]+)/);
          if (salesMatch) {
            salesVolume = `Đã bán ${salesMatch[1]}`;
          }
        }
        
        // Extract image
        let imageUrl = null;
        const sourceEl = card.querySelector('picture source');
        if (sourceEl) {
          const srcset = sourceEl.getAttribute('srcset');
          if (srcset) {
            imageUrl = srcset.split(',')[0].split(' ')[0].trim();
          }
        }
        if (!imageUrl) {
          const imgEl = card.querySelector('img[src*="tikicdn"]') || card.querySelector('img');
          if (imgEl) {
            imageUrl = imgEl.src || imgEl.getAttribute('srcset')?.split(' ')[0];
          }
        }
        
        // TikiNow badge
        let tikiNow = false;
        const images = card.querySelectorAll('img');
        for (const img of images) {
          const src = img.src || '';
          if (src.toLowerCase().includes('tikinow') || src.toLowerCase().includes('tiki-now')) {
            tikiNow = true;
            break;
          }
        }
        
        results.push({
          product_id: productId,
          spid: spid,
          name: name,
          price_raw: priceRaw,
          discount_raw: discountRaw,
          sales_volume: salesVolume,
          rating_raw: null, // Not available in HTML
          review_raw: null, // Not available in HTML
          tiki_now: tikiNow,
          product_url: productUrl,
          image_url: imageUrl,
        });
        
      } catch (cardError) {
        console.error('Error extracting card:', cardError.message);
        continue;
      }
    }
    
    return results;
  });
  
  // Clean and format (HTML fallback - no breadcrumbs available)
  return products.map(product => ({
    product_id: product.product_id,
    seller_product_id: product.spid,
    name: product.name,
    price: cleanPrice(product.price_raw),
    discount_rate: cleanDiscount(product.discount_raw),
    sales_volume: product.sales_volume,
    rating_average: null, // Not available in HTML fallback
    review_count: null, // Not available in HTML fallback
    tiki_now: product.tiki_now,
    product_url: product.product_url,
    image_url: product.image_url,
    
    // Category fields - NOT available in HTML fallback
    category_id: null,
    category_name: null,
    root_category_id: null,
    root_category_name: null,
    category_depth: 0,
    category_path: null,
    
    _extracted_at: new Date().toISOString(),
  }));
}

/**
 * Get total number of pages available
 * @param {Page} page - Puppeteer page instance
 * @returns {Promise<number>} Total pages or 1 if unknown
 */
export async function getTotalPages(page) {
  try {
    const totalPages = await page.evaluate(() => {
      const paginationItems = document.querySelectorAll('[data-view-id*="pagination"], [class*="pagination"] a');
      
      let maxPage = 1;
      for (const item of paginationItems) {
        const text = item.textContent?.trim();
        const pageNum = parseInt(text, 10);
        if (!isNaN(pageNum) && pageNum > maxPage) {
          maxPage = pageNum;
        }
      }
      
      return maxPage;
    });
    
    return totalPages;
  } catch (error) {
    logError('getTotalPages', error);
    return 1;
  }
}

export default {
  extractProducts,
  getTotalPages,
};
