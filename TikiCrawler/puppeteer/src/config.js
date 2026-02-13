/**
 * Configuration for Tiki Crawler
 * Vietnam E-commerce Analytics Platform - Phase 1: Ingestion
 */

// =============================================================================
// CRAWL TARGET CONFIGURATION
// =============================================================================

/**
 * Target category URLs to crawl
 * Array of category URLs - crawler will iterate through all categories
 * Examples:
 * - Điện thoại: https://tiki.vn/dien-thoai-may-tinh-bang/c1789
 * - Laptop: https://tiki.vn/laptop-may-vi-tinh-linh-kien/c1846
 * - Tai nghe: https://tiki.vn/tai-nghe/c8318
 */
export const CATEGORY_URLS = [
  'https://tiki.vn/dien-thoai-may-tinh-bang/c1789',
  'https://tiki.vn/laptop-may-vi-tinh-linh-kien/c1846',
  'https://tiki.vn/thiet-bi-am-thanh-va-phu-kien/c8215',
  'https://tiki.vn/phu-kien-may-tinh-va-laptop/c28670',
  'https://tiki.vn/thiet-bi-thong-minh-va-linh-kien-dien-tu/c28432',
];

// Legacy single category URL (for backward compatibility)
export const CATEGORY_URL = CATEGORY_URLS[0];

/**
 * Maximum number of pages to crawl
 * Set to Infinity to crawl all available pages
 */
export const MAX_PAGES = 10;

// =============================================================================
// BROWSER CONFIGURATION
// =============================================================================

export const BROWSER_CONFIG = {
  headless: 'new', // Use 'new' for headless, false to show browser for debugging
  args: [
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-dev-shm-usage',
    '--disable-accelerated-2d-canvas',
    '--disable-gpu',
    '--window-size=1920,1080',
    '--disable-web-security',
    '--disable-features=IsolateOrigins,site-per-process',
    '--lang=vi-VN,vi',
  ],
  defaultViewport: {
    width: 1920,
    height: 1080,
  },
  ignoreHTTPSErrors: true,
};

// =============================================================================
// TIMING CONFIGURATION (Human-like behavior)
// =============================================================================

export const TIMING = {
  // Page load timeout (ms)
  pageLoadTimeout: 60000,
  
  // Wait for content after page load (ms)
  contentWaitTime: 3000,
  
  // Delay range between scroll actions (ms) [min, max]
  scrollDelay: [300, 800],
  
  // Delay range between page navigations (ms) [min, max]
  pageNavigationDelay: [2000, 5000],
  
  // Scroll step size in pixels
  scrollStep: 800,
  
  // Maximum scroll attempts before giving up (increased for infinite scroll)
  maxScrollAttempts: 200,
  
  // Delay after reaching page bottom (ms)
  bottomWaitTime: 2000,
};

// =============================================================================
// OUTPUT CONFIGURATION
// =============================================================================

export const OUTPUT = {
  // Base directory for raw data output
  baseDir: './data/raw/tiki',
  
  // Date format for folder naming (YYYYMMDD)
  dateFormat: () => {
    const now = new Date();
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    return `${year}${month}${day}`;
  },
};

// =============================================================================
// SELECTORS CONFIGURATION
// =============================================================================
// NOTE: Tiki frequently changes their CSS classes. Update these selectors when needed.
// The selectors are organized by purpose for easy maintenance.

export const SELECTORS = {
  // Product grid container
  productGrid: '[data-view-id="product_list_container"]',
  
  // Individual product card - multiple fallback selectors
  productCard: [
    'a[data-view-id="product_list_item"]',
    '[class*="ProductItem"]',
    'a[href*="/p/"]',
  ],
  
  // Product name/title
  productName: [
    '[class*="name"]',
    '[class*="title"]',
    'h3',
    '[class*="Name"]',
  ],
  
  // Price selectors (current price)
  productPrice: [
    '[class*="price-discount__price"]',
    '[class*="final-price"]',
    '[class*="Price"]',
    '[class*="price"]',
  ],
  
  // Original price (before discount)
  originalPrice: [
    '[class*="original-price"]',
    '[class*="list-price"]',
    'del',
  ],
  
  // Discount badge
  discountBadge: [
    '[class*="badge-under-price"]',
    '[class*="discount"]',
    '[class*="Discount"]',
    '[class*="sale-tag"]',
  ],
  
  // Sales volume (Đã bán)
  salesVolume: [
    '[class*="quantity"]',
    '[class*="sold"]',
    ':contains("Đã bán")',
  ],
  
  // Rating stars
  rating: [
    '[class*="rating"]',
    '[class*="star"]',
    '[class*="Rating"]',
  ],
  
  // Review count
  reviewCount: [
    '[class*="review"]',
    '[class*="Review"]',
  ],
  
  // TikiNow badge
  tikiNowBadge: [
    '[class*="tiki-now"]',
    '[class*="TikiNow"]',
    '[class*="tikiBadge"]',
    'img[src*="tiki-now"]',
    'img[alt*="tikinow"]',
  ],
  
  // Product image
  productImage: [
    'img[class*="thumbnail"]',
    'img[class*="product-image"]',
    'img[src*="tikicdn"]',
    'img',
  ],
  
  // Pagination
  pagination: {
    nextButton: [
      'a[data-view-id="product_list_pagination_item_next"]',
      '[class*="next"]',
      'a[rel="next"]',
    ],
    pageNumbers: '[data-view-id*="pagination"]',
    currentPage: '[class*="active"]',
  },
};

// =============================================================================
// GOOGLE CLOUD STORAGE CONFIGURATION
// =============================================================================

export const GCS_CONFIG = {
  // Path to Service Account key file (absolute path on server)
  keyFilePath: process.env.GCP_CREDENTIALS_PATH || '/opt/airflow/config/google_cloud_credentials.json',
  
  // GCS Bucket name - Read from environment variable
  bucketName: process.env.GCS_BUCKET_NAME || 'gcs-bucket-name',
  
  // Project ID (optional, will be read from key file if not specified)
  projectId: process.env.GCP_PROJECT_ID || null,
  
  // Raw Zone path structure for Hive-style partitioning
  // Format: raw_zone/tiki/products/snapshot_date=YYYY-MM-DD/filename.json
  rawZonePath: 'raw_zone/tiki/products',
  
  // Whether to delete local files after successful GCS upload
  deleteLocalAfterUpload: true,
};

export default {
  CATEGORY_URL,
  CATEGORY_URLS,
  MAX_PAGES,
  BROWSER_CONFIG,
  TIMING,
  OUTPUT,
  SELECTORS,
  GCS_CONFIG,
};
