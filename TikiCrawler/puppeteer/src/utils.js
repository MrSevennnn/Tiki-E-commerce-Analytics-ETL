/**
 * Utility functions for Tiki Crawler
 */

import fs from 'fs';
import path from 'path';
import { TIMING, OUTPUT } from './config.js';

// =============================================================================
// RANDOM DELAY UTILITIES (Human-like behavior)
// =============================================================================

/**
 * Generate a random delay within a range
 * @param {number} min - Minimum delay in ms
 * @param {number} max - Maximum delay in ms
 * @returns {number} Random delay in ms
 */
export function getRandomDelay(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

/**
 * Sleep for a random duration (human-like behavior)
 * @param {number[]} range - [min, max] delay range in ms
 * @returns {Promise<void>}
 */
export async function randomSleep(range = TIMING.scrollDelay) {
  const delay = getRandomDelay(range[0], range[1]);
  await new Promise(resolve => setTimeout(resolve, delay));
}

/**
 * Sleep for a fixed duration
 * @param {number} ms - Duration in milliseconds
 * @returns {Promise<void>}
 */
export async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// =============================================================================
// FILE SYSTEM UTILITIES
// =============================================================================

/**
 * Ensure directory exists, create if not
 * @param {string} dirPath - Directory path
 */
export function ensureDirectoryExists(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
    console.log(`Created directory: ${dirPath}`);
  }
}

/**
 * Get output directory path for today's crawl
 * @returns {string} Output directory path
 */
export function getOutputDirectory() {
  const dateFolder = OUTPUT.dateFormat();
  const outputDir = path.join(OUTPUT.baseDir, dateFolder);
  ensureDirectoryExists(outputDir);
  return outputDir;
}

/**
 * Save data to JSON file
 * @param {string} filePath - Full file path
 * @param {Array|Object} data - Data to save
 */
export function saveToJson(filePath, data) {
  const jsonContent = JSON.stringify(data, null, 2);
  fs.writeFileSync(filePath, jsonContent, 'utf-8');
  console.log(`Saved: ${filePath}`);
}

/**
 * Generate filename for category page
 * @param {string} categoryName - Category name (slug)
 * @param {number} pageNumber - Page number
 * @returns {string} Filename
 */
export function generateFilename(categoryName, pageNumber) {
  // Sanitize category name for filename
  const sanitized = categoryName
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
  
  return `${sanitized}_page_${pageNumber}.json`;
}

// =============================================================================
// URL UTILITIES
// =============================================================================

/**
 * Extract category name from Tiki URL
 * @param {string} url - Category URL
 * @returns {string} Category name
 */
export function extractCategoryName(url) {
  try {
    const urlObj = new URL(url);
    const pathParts = urlObj.pathname.split('/').filter(Boolean);
    
    // Get the category slug (usually the first part before /c{id})
    if (pathParts.length > 0) {
      // Remove the category ID part if present (e.g., "c1789")
      const categorySlug = pathParts[0];
      return categorySlug.replace(/^c\d+$/, '') || categorySlug;
    }
    
    return 'unknown_category';
  } catch (error) {
    console.error('Error extracting category name:', error.message);
    return 'unknown_category';
  }
}

/**
 * Build page URL with page number
 * @param {string} baseUrl - Base category URL
 * @param {number} pageNumber - Page number
 * @returns {string} Full URL with page parameter
 */
export function buildPageUrl(baseUrl, pageNumber) {
  try {
    const url = new URL(baseUrl);
    url.searchParams.set('page', pageNumber.toString());
    return url.toString();
  } catch (error) {
    // Fallback: append page parameter
    const separator = baseUrl.includes('?') ? '&' : '?';
    return `${baseUrl}${separator}page=${pageNumber}`;
  }
}

// =============================================================================
// DATA CLEANING UTILITIES
// =============================================================================

/**
 * Clean price string to number
 * Removes currency symbols, dots, and spaces
 * @param {string} priceStr - Raw price string (e.g., "1.290.000 ₫")
 * @returns {number|null} Price as number or null
 */
export function cleanPrice(priceStr) {
  if (!priceStr) return null;
  
  try {
    // Remove currency symbol, dots, spaces, and non-numeric chars
    const cleaned = priceStr
      .replace(/[₫đ\s.]/gi, '')
      .replace(/,/g, '')
      .trim();
    
    const price = parseInt(cleaned, 10);
    return isNaN(price) ? null : price;
  } catch (error) {
    return null;
  }
}

/**
 * Extract discount rate from text
 * @param {string} discountStr - Discount string (e.g., "-25%")
 * @returns {string|null} Discount rate or null
 */
export function cleanDiscount(discountStr) {
  if (!discountStr) return null;
  
  try {
    // Match percentage pattern
    const match = discountStr.match(/-?\d+%/);
    return match ? match[0] : null;
  } catch (error) {
    return null;
  }
}

/**
 * Extract rating number from text or element
 * @param {string} ratingStr - Rating string
 * @returns {number|null} Rating as number or null
 */
export function cleanRating(ratingStr) {
  if (!ratingStr) return null;
  
  try {
    const match = ratingStr.match(/(\d+\.?\d*)/);
    if (match) {
      const rating = parseFloat(match[1]);
      return rating >= 0 && rating <= 5 ? rating : null;
    }
    return null;
  } catch (error) {
    return null;
  }
}

/**
 * Extract review count from text
 * @param {string} reviewStr - Review string (e.g., "(1.234 đánh giá)")
 * @returns {number|null} Review count or null
 */
export function cleanReviewCount(reviewStr) {
  if (!reviewStr) return null;
  
  try {
    // Remove parentheses and non-numeric except dots for thousands
    const cleaned = reviewStr.replace(/[()đánh giá\s]/gi, '');
    
    // Handle "k" suffix for thousands
    if (cleaned.toLowerCase().includes('k')) {
      const num = parseFloat(cleaned.replace(/k/i, ''));
      return isNaN(num) ? null : Math.round(num * 1000);
    }
    
    // Handle dot as thousands separator
    const num = parseInt(cleaned.replace(/\./g, ''), 10);
    return isNaN(num) ? null : num;
  } catch (error) {
    return null;
  }
}

/**
 * Extract product ID from URL or data attribute
 * @param {string} urlOrId - Product URL or ID string
 * @returns {string|null} Product ID or null
 */
export function extractProductId(urlOrId) {
  if (!urlOrId) return null;
  
  try {
    // If it's already an ID (numeric string)
    if (/^\d+$/.test(urlOrId)) {
      return urlOrId;
    }
    
    // Extract from URL pattern: /p{id} or spid={id}
    const pidMatch = urlOrId.match(/\/p(\d+)|spid=(\d+)|product\/(\d+)/i);
    if (pidMatch) {
      return pidMatch[1] || pidMatch[2] || pidMatch[3];
    }
    
    return null;
  } catch (error) {
    return null;
  }
}

// =============================================================================
// LOGGING UTILITIES
// =============================================================================

/**
 * Log crawler progress
 * @param {number} current - Current page
 * @param {number} total - Total pages
 * @param {number} productCount - Products found on page
 */
export function logProgress(current, total, productCount) {
  const totalStr = total === Infinity ? '∞' : total;
  console.log(`\n[PROGRESS] Crawled page ${current}/${totalStr}: Found ${productCount} products`);
}

/**
 * Log error with context
 * @param {string} context - Error context
 * @param {Error} error - Error object
 */
export function logError(context, error) {
  console.error(`\n[ERROR] ${context}: ${error.message}`);
  if (process.env.DEBUG) {
    console.error(error.stack);
  }
}

/**
 * Log success message
 * @param {string} message - Success message
 */
export function logSuccess(message) {
  console.log(`\n[SUCCESS] ${message}`);
}

/**
 * Log info message
 * @param {string} message - Info message
 */
export function logInfo(message) {
  console.log(`[INFO] ${message}`);
}

export default {
  getRandomDelay,
  randomSleep,
  sleep,
  ensureDirectoryExists,
  getOutputDirectory,
  saveToJson,
  generateFilename,
  extractCategoryName,
  buildPageUrl,
  cleanPrice,
  cleanDiscount,
  cleanRating,
  cleanReviewCount,
  extractProductId,
  logProgress,
  logError,
  logSuccess,
  logInfo,
};
