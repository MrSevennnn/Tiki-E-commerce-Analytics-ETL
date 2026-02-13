/**
 * Browser management module using Puppeteer with Stealth plugin
 * Handles browser initialization and anti-detection measures
 */

import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import { BROWSER_CONFIG, TIMING } from './config.js';
import { randomSleep, sleep, logInfo, logError } from './utils.js';

// Add stealth plugin to avoid detection
puppeteer.use(StealthPlugin());

// =============================================================================
// BROWSER INITIALIZATION
// =============================================================================

/**
 * Initialize browser with stealth configuration
 * @returns {Promise<Browser>} Puppeteer browser instance
 */
export async function initBrowser() {
  logInfo('Initializing browser with stealth mode...');
  
  const browser = await puppeteer.launch(BROWSER_CONFIG);
  
  logInfo('Browser launched successfully');
  return browser;
}

/**
 * Create a new page with anti-detection measures
 * @param {Browser} browser - Puppeteer browser instance
 * @returns {Promise<Page>} Configured page instance
 */
export async function createPage(browser) {
  const page = await browser.newPage();
  
  await page.setExtraHTTPHeaders({
    'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
  });
  
  await page.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
  );
  
  await page.setViewport(BROWSER_CONFIG.defaultViewport);
  
  page.setDefaultNavigationTimeout(TIMING.pageLoadTimeout);
  page.setDefaultTimeout(TIMING.pageLoadTimeout);
  
  return page;
}

// =============================================================================
// PAGE SCROLLING (Lazy Loading / Infinite Scroll Handler)
// =============================================================================

/**
 * Scroll to bottom of page to trigger lazy loading
 * Implements human-like scrolling behavior with random delays
 * Handles Tiki's infinite scroll - keeps scrolling until no new products load
 * @param {Page} page - Puppeteer page instance
 */
export async function scrollToBottom(page) {
  logInfo('Scrolling page to load all products (infinite scroll)...');
  
  let previousProductCount = 0;
  let sameCountAttempts = 0;
  let scrollAttempts = 0;
  const maxSameCountAttempts = 5; // Stop after 5 attempts with no new products
  
  while (scrollAttempts < TIMING.maxScrollAttempts) {
    // Count current products on page
    const currentProductCount = await page.evaluate(() => {
      return document.querySelectorAll('.product-item').length;
    });
    
    // Log progress every 10 scrolls
    if (scrollAttempts % 10 === 0 && scrollAttempts > 0) {
      logInfo(`  ...scrolled ${scrollAttempts} times, found ${currentProductCount} products so far`);
    }
    
    // Check if new products were loaded
    if (currentProductCount === previousProductCount) {
      sameCountAttempts++;
      
      // If no new products after multiple attempts, we're done
      if (sameCountAttempts >= maxSameCountAttempts) {
        logInfo(`No new products after ${maxSameCountAttempts} scroll attempts. Done loading.`);
        break;
      }
      
      // Try scrolling more aggressively
      await page.evaluate(() => {
        window.scrollTo(0, document.body.scrollHeight);
      });
      await sleep(2000); // Wait longer for content to load
      
    } else {
      // New products loaded, reset counter
      sameCountAttempts = 0;
      previousProductCount = currentProductCount;
    }
    
    // Scroll down by a larger step for faster loading
    await page.evaluate(() => {
      window.scrollBy({
        top: 800,
        behavior: 'smooth'
      });
    });
    
    // Random delay between scrolls (human-like)
    await randomSleep(TIMING.scrollDelay);
    
    scrollAttempts++;
  }
  
  // Final count
  const finalCount = await page.evaluate(() => {
    return document.querySelectorAll('.product-item').length;
  });
  
  logInfo(`Finished scrolling. Total products loaded: ${finalCount}`);
  
  // Wait a bit at the bottom for any final content to load
  await sleep(TIMING.bottomWaitTime);
  
  // Scroll back to top
  await page.evaluate(() => window.scrollTo({ top: 0, behavior: 'smooth' }));
  await sleep(500);
}

// =============================================================================
// NAVIGATION
// =============================================================================

/**
 * Navigate to URL with error handling
 * @param {Page} page - Puppeteer page instance
 * @param {string} url - URL to navigate to
 * @returns {Promise<boolean>} Success status
 */
export async function navigateToPage(page, url) {
  try {
    logInfo(`Navigating to: ${url}`);
    
    await page.goto(url, {
      waitUntil: 'networkidle2',
      timeout: TIMING.pageLoadTimeout,
    });
    
    // Wait for content to render
    await sleep(TIMING.contentWaitTime);
    
    return true;
  } catch (error) {
    logError('Navigation', error);
    return false;
  }
}

/**
 * Wait for product grid to appear
 * @param {Page} page - Puppeteer page instance
 * @param {string[]} selectors - Array of possible selectors
 * @returns {Promise<boolean>} Success status
 */
export async function waitForProducts(page, selectors) {
  for (const selector of selectors) {
    try {
      await page.waitForSelector(selector, { timeout: 10000 });
      logInfo(`Found products using selector: ${selector}`);
      return true;
    } catch (error) {
      // Try next selector
      continue;
    }
  }
  
  logError('waitForProducts', new Error('No product selectors matched'));
  return false;
}

// =============================================================================
// PAGINATION
// =============================================================================

/**
 * Check if next page exists
 * @param {Page} page - Puppeteer page instance
 * @param {string[]} nextButtonSelectors - Array of possible next button selectors
 * @returns {Promise<boolean>} Whether next page exists
 */
export async function hasNextPage(page, nextButtonSelectors) {
  for (const selector of nextButtonSelectors) {
    try {
      const nextButton = await page.$(selector);
      if (nextButton) {
        // Check if button is disabled
        const isDisabled = await page.evaluate((el) => {
          return el.hasAttribute('disabled') || 
                 el.classList.contains('disabled') ||
                 el.getAttribute('aria-disabled') === 'true';
        }, nextButton);
        
        if (!isDisabled) {
          return true;
        }
      }
    } catch (error) {
      continue;
    }
  }
  
  return false;
}

/**
 * Get current page number from URL or page content
 * @param {Page} page - Puppeteer page instance
 * @returns {Promise<number>} Current page number
 */
export async function getCurrentPageNumber(page) {
  try {
    const url = page.url();
    const urlObj = new URL(url);
    const pageParam = urlObj.searchParams.get('page');
    
    if (pageParam) {
      return parseInt(pageParam, 10);
    }
    
    return 1;
  } catch (error) {
    return 1;
  }
}

// =============================================================================
// CLEANUP
// =============================================================================

/**
 * Close browser gracefully
 * @param {Browser} browser - Puppeteer browser instance
 */
export async function closeBrowser(browser) {
  if (browser) {
    await browser.close();
    logInfo('Browser closed');
  }
}

export default {
  initBrowser,
  createPage,
  scrollToBottom,
  navigateToPage,
  waitForProducts,
  hasNextPage,
  getCurrentPageNumber,
  closeBrowser,
};
