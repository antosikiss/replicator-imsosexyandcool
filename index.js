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

async function pollWavespeedResult(requestId, maxAttempts = 60, interval = 5000) {
  for (let i = 0; i < maxAttempts; i++) {
    await new Promise(resolve => setTimeout(resolve, interval));
    const pollRes = await fetch(`https://api.wavespeed.ai/api/v3/predictions/${requestId}/result`, {
      headers: { 'Authorization': `Bearer ${WAVESPEED_API_KEY}` }
    });
    if (!pollRes.ok) continue;
    const pollJson = await pollRes.json();
    if (pollJson.status === 'completed' || pollJson.output) {
      return pollJson;
    }
    if (pollJson.status === 'failed') {
      throw new Error('Wavespeed job failed');
    }
  }
  throw new Error('Wavespeed timeout');
}

async function handleGenerate(recordId, res) {
  console.log('handleGenerate called with recordId:', recordId);
  if (!recordId) return res.status(400).send('Missing recordId');

  const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY;
  const AIRTABLE_BASE_ID = 'app5JstpSmtghcbMA';
  const WAVESPEED_API_KEY = process.env.WAVESPEED_API_KEY;
  const APIFY_API_KEY = process.env.APIFY_API_KEY;
  const MAIN_TABLE_NAME = 'Generation';

  if (!AIRTABLE_API_KEY || !WAVESPEED_API_KEY) return res.status(500).send('Missing required env vars');

  const base = new Airtable({ apiKey: AIRTABLE_API_KEY }).base(AIRTABLE_BASE_ID);

  try {
    console.log('Fetching record:', recordId);
    const record = await base(MAIN_TABLE_NAME).find(recordId);
    const fields = record.fields;

    if (!fields.Generate) return res.status(200).send('Generate not triggered');

    await base(MAIN_TABLE_NAME).update(recordId, { Status: 'Generating' });

    let sourceVideoUrl = fields['Source_Video'] ? fields['Source_Video'][0].url : null;
    let coverImageUrl = fields['Cover_Image'] ? fields['Cover_Image'][0].url : null;
    const tiktokLink = fields.Link;
    const aiCharacterUrl = fields['AI_Character'] ? fields['AI_Character'][0].url : null;

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
      if (apifyJson.length === 0) throw new Error('Empty Apify response');
      const post = apifyJson[0];
      sourceVideoUrl = post.playAddr || post.videoMeta?.playAddr || post.downloadAddr || post.webVideoUrl || post.videoUrl;
      coverImageUrl = post.cover || post.videoMeta?.cover || post.originCover || post.dynamicCover;
      if (!sourceVideoUrl) throw new Error('No video URL found in Apify response');

      // Force .mp4 for Airtable preview
      if (!sourceVideoUrl.endsWith('.mp4')) sourceVideoUrl += '.mp4';
    }

    if (!sourceVideoUrl) throw new Error('Missing Source Video');

    // Update Source_Video and Cover_Image
    await base(MAIN_TABLE_NAME).update(recordId, {
      'Source_Video': [{ url: sourceVideoUrl }],
      'Cover_Image': coverImageUrl ? [{ url: coverImageUrl }] : []
    });

    // Fallback to coverImageUrl if aiCharacterUrl is missing
    const faceImageUrl = aiCharacterUrl || coverImageUrl;
    if (!faceImageUrl) throw new Error('Missing AI_Character or Cover_Image for face swap');

    // Generate faces with Seedream v4.5 on Wavespeed (async)
    console.log('Generating images with Seedream v4.5 on Wavespeed');
    const seedreamUuid = 'bytedance/seedream-v4.5/edit';
    const seedreamUrl = `https://api.wavespeed.ai/api/v3/${seedreamUuid}`;
    const seedreamData = {
      images: [faceImageUrl],
      prompt: 'high quality portrait, detailed face, realistic skin, sharp eyes',
      width: 1728,
      height: 2304
    };
    const seedreamRes = await fetch(seedreamUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${WAVESPEED_API_KEY}`
      },
      body: JSON.stringify(seedreamData)
    });
    if (!seedreamRes.ok) throw new Error(`Seedream error: ${seedreamRes.statusText}`);
    const seedreamJson = await seedreamRes.json();
    const seedreamRequestId = seedreamJson.id || seedreamJson.requestId;
    const seedreamResult = await pollWavespeedResult(seedreamRequestId);
    const generatedImages = (seedreamResult.output || []).map(url => ({ url }));
    if (generatedImages.length === 0) throw new Error('No generated images from Seedream');

    // Update Generated_Images
    await base(MAIN_TABLE_NAME).update(recordId, { 'Generated_Images': generatedImages });

    // Animate/face swap with Wan 2.2 Animate on Wavespeed (async)
    console.log('Performing animation with Wan 2.2 Animate on Wavespeed');
    const wanUuid = 'wavespeed-ai/wan-2.2/animate';
    const wanUrl = `https://api.wavespeed.ai/api/v3/${wanUuid}`;
    const wanData = {
      image: generatedImages[0].url,
      video: sourceVideoUrl,
      mode: 'animate',
      resolution: '720p'
    };
    const wanRes = await fetch(wanUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${WAVESPEED_API_KEY}`
      },
      body: JSON.stringify(wanData)
    });
    if (!wanRes.ok) throw new Error(`Wan Animate error: ${wanRes.statusText}`);
    const wanJson = await wanRes.json();
    const wanRequestId = wanJson.id || wanJson.requestId;
    const wanResult = await pollWavespeedResult(wanRequestId);
    const outputVideoUrl = wanResult.output_video_url;

    // Success update
    await base(MAIN_TABLE_NAME).update(recordId, {
      'Output_Video': [{ url: outputVideoUrl }],
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

async function pollWavespeedResult(requestId, maxAttempts = 60, interval = 5000) {
  for (let i = 0; i < maxAttempts; i++) {
    await new Promise(resolve => setTimeout(resolve, interval));
    const pollRes = await fetch(`https://api.wavespeed.ai/api/v3/predictions/${requestId}/result`, {
      headers: { 'Authorization': `Bearer ${WAVESPEED_API_KEY}` }
    });
    if (!pollRes.ok) continue;
    const pollJson = await pollRes.json();
    if (pollJson.status === 'completed' || pollJson.output) {
      return pollJson;
    }
    if (pollJson.status === 'failed') {
      throw new Error('Wavespeed job failed');
    }
  }
  throw new Error('Wavespeed timeout');
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
