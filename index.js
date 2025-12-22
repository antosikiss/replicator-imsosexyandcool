const express = require('express');
const fetch = require('node-fetch');
const Airtable = require('airtable');
const app = express();
app.use(express.json());

// Global handlers
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection:', reason ? reason.stack || reason : 'No reason');
});
process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error.stack || error);
});

app.get('/generate', async (req, res) => {
  console.log('GET /generate received with query:', JSON.stringify(req.query));
  try {
    const recordId = req.query.recordId;
    await handleGenerate(recordId, res);
  } catch (error) {
    console.error('Error in GET /generate:', error.stack || error);
    res.status(500).send('Server error');
  }
});

app.post('/generate', async (req, res) => {
  console.log('POST /generate received with body:', JSON.stringify(req.body));
  try {
    const { recordId } = req.body;
    await handleGenerate(recordId, res);
  } catch (error) {
    console.error('Error in POST /generate:', error.stack || error);
    res.status(500).send('Server error');
  }
});

async function handleGenerate(recordId, res) {
  console.log('handleGenerate called with recordId:', recordId);
  if (!recordId) return res.status(400).send('Missing recordId');

  const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY;
  const AIRTABLE_BASE_ID = 'app5JstpSmtghcbMA';
  const WAVESPEED_API_KEY = process.env.WAVESPEED_API_KEY;
  const APIFY_API_KEY = process.env.APIFY_API_KEY;
  const FAL_AI_API_KEY = process.env.FAL_AI_API_KEY;
  const MAIN_TABLE_NAME = 'Generation';

  if (!AIRTABLE_API_KEY || !WAVESPEED_API_KEY) return res.status(500).send('Missing required env vars');

  const base = new Airtable({ apiKey: AIRTABLE_API_KEY }).base(AIRTABLE_BASE_ID);

  try {
    console.log('Fetching record:', recordId);
    const record = await base(MAIN_TABLE_NAME).find(recordId);
    const fields = record.fields;

    if (!fields.Generate) return res.status(200).send('Generate not triggered');

    await base(MAIN_TABLE_NAME).update(recordId, { Status: 'Generating' });

    let sourceVideoUrl = fields['Source Video'] ? fields['Source Video'][0].url : null;
    let coverImageUrl = fields['Cover Image'] ? fields['Cover Image'][0].url : null;
    const tiktokLink = fields.Link;
    const aiCharacterUrl = fields['AI Character'] ? fields['AI Character'][0].url : null;

    if ((!sourceVideoUrl || !coverImageUrl) && tiktokLink && tiktokLink.includes('tiktok.com') && APIFY_API_KEY) {
      console.log('Downloading video and thumbnail from Apify');
      const apifyData = {
        urls: [tiktokLink],
        format: 'mp4',
        watermark: false
      };
      const apifyUrl = `https://api.apify.com/v2/acts/dz_omar~tiktok-video-downloader/run-sync-get-dataset-items?token=${APIFY_API_KEY}`;
      const apifyRes = await fetch(apifyUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(apifyData)
      });
      if (!apifyRes.ok) throw new Error(`Apify error: ${apifyRes.statusText}`);
      const apifyJson = await apifyRes.json();
      console.log('Apify response:', JSON.stringify(apifyJson));
      sourceVideoUrl = apifyJson[0]?.video_download_url_no_watermark || apifyJson[0]?.video_url;
      coverImageUrl = apifyJson[0]?.thumbnail_url || apifyJson[0]?.cover_image;
      if (!sourceVideoUrl) throw new Error('No video URL found in Apify response');
    }

    if (!sourceVideoUrl) throw new Error('Missing Source Video');
    if (!aiCharacterUrl) throw new Error('Missing AI Character image');

    // Update Source Video and Cover Image
    await base(MAIN_TABLE_NAME).update(recordId, {
      'Source Video': [{ url: sourceVideoUrl }],
      'Cover Image': coverImageUrl ? [{ url: coverImageUrl }] : []
    });

    // Generate faces with Seedream via fal.ai
    console.log('Generating images with Seedream');
    const seedreamUrl = 'https://fal.ai/models/fal-ai/bytedance/seedream/v4/edit';
    const seedreamData = {
      prompt: 'Generate high-quality variations of this face, detailed, realistic, same pose and style',
      image_urls: [aiCharacterUrl],
      image_size: { width: 1728, height: 2304 },
      num_images: 1,
      enable_safety_checker: true
    };
    const seedreamRes = await fetch(seedreamUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Key ${FAL_AI_API_KEY}`
      },
      body: JSON.stringify(seedreamData)
    });
    if (!seedreamRes.ok) throw new Error(`Seedream error: ${seedreamRes.statusText}`);
    const seedreamJson = await seedreamRes.json();
    const generatedImages = seedreamJson.images ? seedreamJson.images.map(img => ({ url: img.url })) : [];

    // Update Generated Images
    await base(MAIN_TABLE_NAME).update(recordId, { 'Generated Images': generatedImages });

    // Face swap video with Wan Animate on fal.ai
    console.log('Performing face swap with Wan Animate');
    const wanAnimateUrl = 'https://fal.ai/models/fal-ai/wan/v2.2-14b/animate/move';
    const wanData = {
      video_url: sourceVideoUrl,
      image_url: generatedImages.length > 0 ? generatedImages[0].url : aiCharacterUrl,  // Use generated or fallback
      resolution: '720p'
    };
    const wanRes = await fetch(wanAnimateUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Key ${FAL_AI_API_KEY}`
      },
      body: JSON.stringify(wanData)
    });
    if (!wanRes.ok) throw new Error(`Wan Animate error: ${wanRes.statusText}`);
    const wanJson = await wanRes.json();
    const outputVideoUrl = wanJson.video_url;  // Adjust based on actual response

    // Success update
    await base(MAIN_TABLE_NAME).update(recordId, {
      'Output Video': [{ url: outputVideoUrl }],
      Status: 'Complete',
      Generate: false
    });
    res.status(200).send('Generation complete');

  } catch (error) {
    console.error('Error during generation:', error.message, error.stack);
    try {
      await base(MAIN_TABLE_NAME).update(recordId, {
        Status: 'Failed',
        'Error Message': error.message || 'Unknown error',
        Generate: false
      });
    } catch (updateError) {
      console.error('Update failed:', updateError.message, updateError.stack);
    }
    res.status(500).send(error.message || 'Unknown error');
  }
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
