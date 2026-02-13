/**
 * Analyze Tiki API Structure
 * This script monitors network requests to identify the API endpoint
 * used by Tiki for product listing pagination
 */

import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import { BROWSER_CONFIG } from './config.js';
import fs from 'fs';

puppeteer.use(StealthPlugin());

const CATEGORY_URL = 'https://tiki.vn/dien-thoai-may-tinh-bang/c1789';

async function analyzeTikiAPI() {
  console.log('Starting Tiki API Analysis...\n');
  
  const browser = await puppeteer.launch({
    ...BROWSER_CONFIG,
    headless: false, // Show browser to observe behavior
  });
  
  const page = await browser.newPage();
  
  // Capture all network requests
  const apiRequests = [];
  
  page.on('request', request => {
    const url = request.url();
    
    // Look for API calls (typically contain 'api', 'v2', 'search', 'product')
    if (url.includes('/api/') || 
        url.includes('/v2/') || 
        url.includes('personalization') ||
        url.includes('product') ||
        url.includes('search')) {
      
      apiRequests.push({
        url: url,
        method: request.method(),
        headers: request.headers(),
        postData: request.postData(),
      });
      
      console.log(`📡 API Request: ${request.method()} ${url}`);
    }
  });
  
  page.on('response', async response => {
    const url = response.url();
    
    // Capture responses from API endpoints
    if (url.includes('/api/') || 
        url.includes('/v2/') ||
        url.includes('personalization') ||
        url.includes('product')) {
      
      try {
        const contentType = response.headers()['content-type'] || '';
        
        if (contentType.includes('application/json')) {
          const responseData = await response.json();
          
          // Look for product data structures
          if (responseData && (responseData.data || responseData.products || responseData.items)) {
            console.log(`\nFound product data in: ${url}`);
            console.log(`Response structure:`, Object.keys(responseData));
            
            // Save interesting responses
            const filename = `api_response_${Date.now()}.json`;
            fs.writeFileSync(
              `./debug_${filename}`,
              JSON.stringify({
                url: url,
                data: responseData
              }, null, 2)
            );
            
            console.log(`Saved to debug_${filename}\n`);
          }
        }
      } catch (err) {
        // Not JSON or error parsing
      }
    }
  });
  
  console.log(`Navigating to: ${CATEGORY_URL}\n`);
  await page.goto(CATEGORY_URL, {
    waitUntil: 'networkidle2',
    timeout: 60000,
  });
  
  console.log('Waiting 5 seconds for initial page load...\n');
  await new Promise(resolve => setTimeout(resolve, 5000));
  
  console.log('Scrolling to trigger pagination...\n');
  
  // Scroll down multiple times to trigger pagination
  for (let i = 0; i < 3; i++) {
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight);
    });
    
    console.log(`Scroll ${i + 1}/3 - waiting for new content...`);
    await new Promise(resolve => setTimeout(resolve, 3000));
  }
  
  console.log('\nAnalysis Summary:');
  console.log(`Total API requests captured: ${apiRequests.length}`);
  
  // Save all API requests for analysis
  fs.writeFileSync(
    './debug_all_api_requests.json',
    JSON.stringify(apiRequests, null, 2)
  );
  
  console.log('\nSaved all requests to: debug_all_api_requests.json');
  console.log('\nCheck the debug_api_response_*.json files for product data structure');
  
  // Keep browser open for manual inspection
  console.log('\n⏸Browser will stay open for 30 seconds for manual inspection...');
  await new Promise(resolve => setTimeout(resolve, 30000));
  
  await browser.close();
  console.log('\nAnalysis complete!');
}

// Run analysis
analyzeTikiAPI().catch(err => {
  console.error('Error during analysis:', err);
  process.exit(1);
});
