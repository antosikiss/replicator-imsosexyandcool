const express = require('express');
const fetch = require('node-fetch');
const Airtable = require('airtable');

const app = express();
app.use(express.json());

// Global handlers to log and prevent crashes from unhandled errors
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});
process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
});

app.get('/generate', async (req, res) => {
  try {
    const recordId = req.query.recordId;
    await handleGenerate(recordId, res);
  } catch (error) {
    console.error('Error in GET /generate:', error);
    res.status(500).send('Server error');
  }
});

app.post('/generate', async (req, res) => {
  try {
    const { recordId } = req.body;
    await handleGenerate(recordId, res);
  } catch (error) {
    console.error('Error in POST /generate:', error);
    res.status(500).send('Server error');
  }
});

async function handleGenerate(recordId, res) {
  if (!recordId) return res.status(400).send('Missing recordId');

  const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY;
  const AIRTABLE_BASE_ID = 'app3cHH00xp68kQQR';  // From your config
  const WAVESPEED_API_KEY = process.env.WAVESPEED_API_KEY;  // From config
  const APIFY_API_KEY = process.env.APIFY_API_KEY;  // For video download
  const FAL_AI_API_KEY = process.env.FAL_AI_API_KEY;  // Optional, if using FAL.ai alternative
  const MAIN_TABLE_NAME = 'Grid view';  // Exact from your table
  const CONFIG_TABLE_NAME = 'Configuration';

  if (!AIRTABLE_API_KEY || !WAVESPEED_API_KEY) return res.status(500).send('Missing env vars');

  const base = new Airtable({ apiKey: AIRTABLE_API_KEY }).base(AIRTABLE_BASE_ID);

  try {
    // Fetch config
    const configRecords = await base(CONFIG_TABLE_NAME).select().firstPage();
    if (!configRecords.length) throw new Error('No config');
    const config = configRecords[0].fields;

    // Fetch record
    const record = await base(MAIN_TABLE_NAME).find(recordId);
    const fields = record.fields;
    if (!fields.Generate) return res.status(200).send('Generate not triggered');

    // Set status to Generating immediately
    await base(MAIN_TABLE_NAME).update(recordId, { Status: 'Generating' });

    let sourceVideoUrl = fields['Source Video'] ? fields['Source Video'][0].url : null;
    const tiktokLink = fields.Link;

    // If no Source Video but Link is TikTok URL, download via Apify
    if (!sourceVideoUrl && tiktokLink.includes('tiktok.com') && APIFY_API_KEY) {
      const apifyData = {
        urls: [tiktokLink],
        format: 'mp4'
      };
      const apifyRes = await fetch('https://api.apify.com/v2/acts/apify/tiktok-scraper/runs?token=' + APIFY_API_KEY, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(apifyData)
      });
      const apifyJson = await apifyRes.json();
      // Poll for Apify result (simplified; add full polling if needed)
      sourceVideoUrl = apifyJson.data.items[0].videoUrl;  // Adapt to actual response
    }
    if (!sourceVideoUrl) throw new Error('Missing Source Video');

    // Generate images (Seedream 4.5)
    const numImages = parseInt(config['# num_images']) || 1;
    const imageSize = config['Image Size'] || '1728x2304';
    const imageOutputs = [];
    for (let i = 0; i < numImages; i++) {
      const imageData = {
        prompt: fields.Link,  // Use Link as prompt (adapt if needed)
        size: imageSize,
        enable_sync_mode: false
      };
      const submitRes = await fetch('https://api.wavespeed.ai/api/v3/bytedance/seedream-v4.5', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${WAVESPEED_API_KEY}` },
        body: JSON.stringify(imageData)
      });
      const submitJson = await submitRes.json();
      if (submitJson.code !== 200) throw new Error('Image submission failed');

      let status = submitJson.data.status;
      let output;
      while (status === 'created' || status === 'processing') {
        await new Promise(resolve => setTimeout(resolve, 5000));
        const pollRes = await fetch(`https://api.wavespeed.ai/api/v3/predictions/${submitJson.data.id}/result`, {
          headers: { 'Authorization': `Bearer ${WAVESPEED_API_KEY}` }
        });
        const pollJson = await pollRes.json();
        status = pollJson.data.status;
        if (status === 'completed') output = pollJson.data.outputs[0];
        if (status === 'failed') throw new Error('Image generation failed');
      }
      imageOutputs.push(output);
    }

    // Generate video (WAN 2.2 Animate)
    const characterImageUrl = imageOutputs[0];
    const videoResolution = config['Video Resolution'] ? config['Video Resolution'].toLowerCase() : '720p';
    const videoData = {
      image: characterImageUrl,
      video: sourceVideoUrl,
      mode: 'replace',
      prompt: fields.Link,
      resolution: videoResolution
    };
    const videoSubmitRes = await fetch('https://api.wavespeed.ai/api/v3/wavespeed-ai/wan-2.2/animate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${WAVESPEED_API_KEY}` },
      body: JSON.stringify(videoData)
    });
    const videoSubmitJson = await videoSubmitRes.json();
    if (videoSubmitJson.code !== 200) throw new Error('Video submission failed');

    let videoStatus = videoSubmitJson.data.status;
    let videoOutput;
    while (videoStatus === 'created' || videoStatus === 'processing') {
      await new Promise(resolve => setTimeout(resolve, 5000));
      const videoPollRes = await fetch(`https://api.wavespeed.ai/api/v3/predictions/${videoSubmitJson.data.id}/result`, {
        headers: { 'Authorization': `Bearer ${WAVESPEED_API_KEY}` }
      });
      const videoPollJson = await videoPollRes.json();
      videoStatus = videoPollJson.data.status;
      if (videoStatus === 'completed') videoOutput = videoPollJson.data.outputs[0];
      if (videoStatus === 'failed') throw new Error('Video generation failed');
    }

    await base(MAIN_TABLE_NAME).update(recordId, {
      Status: 'Success',
      'Generated Images': imageOutputs.map(url => ({ url })),
      'Output Video': [{ url: videoOutput }],
      Generate: false
    });

    res.status(200).send('Generation completed');
  } catch (error) {
    console.error('Error during generation:', error.message);
    await base(MAIN_TABLE_NAME).update(recordId, {
      Status: 'Failed',
      'Error Message': error.message,
      Generate: false
    });
    res.status(500).send(error.message);
  }
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
