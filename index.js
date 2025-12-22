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
        urls: [tiktokLink]
      };
      const apifyUrl = `https://api.apify.com/v2/acts/S5h7zRLfKFEr8pdj7/run-sync-get-dataset-items?token=${APIFY_API_KEY}`;
      const apifyRes = await fetch(apifyUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(apifyData)
      });
      if (!apifyRes.ok) {
        const errText = await apifyRes.text();
        throw new Error(`Apify error: ${apifyRes.statusText} - ${errText}`);
      }
      const apifyJson = await apifyRes.json();
      console.log('Apify response:', JSON.stringify(apifyJson));
      sourceVideoUrl = apifyJson[0]?.playAddr || apifyJson[0]?.video_download_url_no_watermark || apifyJson[0]?.downloadAddr;
      coverImageUrl = apifyJson[0]?.cover || apifyJson[0]?.originCover || apifyJson[0]?.thumbnail_url;
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
    const seedreamUrl = 'https://fal.ai/models/fal-ai/bytedance/seedream/v4.5/edit';
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

    // Face swap video with Wavespeed
    console.log('Performing face swap with Wavespeed');
    const wavespeedUrl = 'https://api.wavespeed.ai/api/v3/video-face-swap';
    const wavespeedData = {
      video_url: sourceVideoUrl,
      face_image_url: generatedImages.length > 0 ? generatedImages[0].url : aiCharacterUrl,
      resolution: '720p'
    };
    const wavespeedRes = await fetch(wavespeedUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${WAVESPEED_API_KEY}`
      },
      body: JSON.stringify(wavespeedData)
    });
    if (!wavespeedRes.ok) throw new Error(`Wavespeed error: ${wavespeedRes.statusText}`);
    const wavespeedJson = await wavespeedRes.json();
    const outputVideoUrl = wavespeedJson.output_video_url;

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
