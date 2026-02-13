/**
 * Google Cloud Storage Integration Module
 * Vietnam E-commerce Analytics Platform - Phase 1: Ingestion
 * 
 * Handles file uploads to GCS with Hive-style partitioning
 * for integration with Airflow DAG (Extract -> Load)
 */

import { Storage } from '@google-cloud/storage';
import fs from 'fs';
import path from 'path';
import { GCS_CONFIG } from './config.js';
import { logInfo, logSuccess, logError } from './utils.js';

// =============================================================================
// GCS CLIENT INITIALIZATION
// =============================================================================

/**
 * Initialize GCS client with service account credentials
 * @returns {Storage} GCS Storage client
 */
function initGCSClient() {
  const keyFilePath = path.resolve(GCS_CONFIG.keyFilePath);
  
  if (!fs.existsSync(keyFilePath)) {
    throw new Error(`GCS credentials file not found: ${keyFilePath}`);
  }
  
  return new Storage({
    keyFilename: keyFilePath,
    projectId: GCS_CONFIG.projectId,
  });
}

// =============================================================================
// DATE UTILITIES
// =============================================================================

/**
 * Get current date in YYYY-MM-DD format for Hive partitioning
 * @returns {string} Date string in YYYY-MM-DD format
 */
export function getSnapshotDate() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

/**
 * Build GCS destination path with Hive-style partitioning
 * Format: raw_zone/tiki/products/snapshot_date=YYYY-MM-DD/filename.json
 * 
 * @param {string} filename - Original filename
 * @param {string} snapshotDate - Optional custom snapshot date (defaults to today)
 * @returns {string} GCS destination path
 */
export function buildGCSDestinationPath(filename, snapshotDate = null) {
  const date = snapshotDate || getSnapshotDate();
  return `${GCS_CONFIG.rawZonePath}/snapshot_date=${date}/${filename}`;
}

// =============================================================================
// UPLOAD FUNCTION
// =============================================================================

/**
 * Upload a local file to Google Cloud Storage
 * 
 * @param {string} localFilePath - Full path to the local file
 * @param {string} destinationPath - GCS destination path (without bucket name)
 * @returns {Promise<{success: boolean, gcsUri: string}>} Upload result
 * @throws {Error} If upload fails
 */
export async function uploadToGCS(localFilePath, destinationPath) {
  const storage = initGCSClient();
  const bucket = storage.bucket(GCS_CONFIG.bucketName);
  
  logInfo(`Uploading to GCS: ${path.basename(localFilePath)}`);
  logInfo(`  Local: ${localFilePath}`);
  logInfo(`  Destination: gs://${GCS_CONFIG.bucketName}/${destinationPath}`);
  
  try {
    if (!fs.existsSync(localFilePath)) {
      throw new Error(`Local file not found: ${localFilePath}`);
    }
    
    await bucket.upload(localFilePath, {
      destination: destinationPath,
      metadata: {
        contentType: 'application/json',
        metadata: {
          uploadedAt: new Date().toISOString(),
          source: 'tiki-crawler',
          snapshotDate: getSnapshotDate(),
        },
      },
    });
    
    const gcsUri = `gs://${GCS_CONFIG.bucketName}/${destinationPath}`;
    logSuccess(`Upload successful: ${gcsUri}`);
    
    return {
      success: true,
      gcsUri: gcsUri,
    };
    
  } catch (error) {
    logError('GCS Upload', error);
    throw error; // Re-throw to let caller handle
  }
}

// =============================================================================
// BATCH UPLOAD FUNCTION
// =============================================================================

/**
 * Upload multiple files to GCS with proper error handling
 * 
 * @param {string[]} localFilePaths - Array of local file paths
 * @param {boolean} deleteOnSuccess - Whether to delete local files after successful upload
 * @returns {Promise<{uploaded: string[], failed: string[]}>} Upload results
 */
export async function uploadMultipleToGCS(localFilePaths, deleteOnSuccess = true) {
  const results = {
    uploaded: [],
    failed: [],
  };
  
  console.log('\n' + '-'.repeat(40));
  logInfo(`Starting GCS upload for ${localFilePaths.length} files...`);
  
  for (const localFilePath of localFilePaths) {
    try {
      const filename = path.basename(localFilePath);
      const destinationPath = buildGCSDestinationPath(filename);
      
      const result = await uploadToGCS(localFilePath, destinationPath);
      
      if (result.success) {
        results.uploaded.push({
          local: localFilePath,
          gcs: result.gcsUri,
        });
        
        // Delete local file on successful upload
        if (deleteOnSuccess) {
          fs.unlinkSync(localFilePath);
          logInfo(`  Deleted local file: ${filename}`);
        }
      }
      
    } catch (error) {
      results.failed.push({
        local: localFilePath,
        error: error.message,
      });
      // DO NOT delete local file on failure
      logError(`Failed to upload ${path.basename(localFilePath)}`, error);
    }
  }
  
  console.log('\n' + '-'.repeat(40));
  logInfo('GCS Upload Summary:');
  logInfo(`  ✓ Uploaded: ${results.uploaded.length} files`);
  if (results.failed.length > 0) {
    logError(`  ✗ Failed: ${results.failed.length} files`, new Error('Some uploads failed'));
  }
  
  return results;
}

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

/**
 * Delete a local file safely
 * @param {string} filePath - Path to file to delete
 * @returns {boolean} True if deleted successfully
 */
export function deleteLocalFile(filePath) {
  try {
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
      logInfo(`Deleted local file: ${path.basename(filePath)}`);
      return true;
    }
    return false;
  } catch (error) {
    logError(`Failed to delete ${filePath}`, error);
    return false;
  }
}

/**
 * Test GCS connection by attempting to access the bucket
 * @returns {Promise<boolean>} True if connection successful
 */
export async function testGCSConnection() {
  try {
    const storage = initGCSClient();
    const bucket = storage.bucket(GCS_CONFIG.bucketName);
    
    // Try to get bucket metadata to verify access
    const [metadata] = await bucket.getMetadata();
    
    logSuccess(`GCS connection successful!`);
    logInfo(`  Bucket: ${metadata.name}`);
    logInfo(`  Location: ${metadata.location}`);
    logInfo(`  Storage Class: ${metadata.storageClass}`);
    
    return true;
  } catch (error) {
    logError('GCS connection test failed', error);
    return false;
  }
}
