const express = require('express');
const fetch = require('node-fetch');
const Airtable = require('airtable');
const cheerio = require('cheerio');
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

    if ((!sourceVideoUrl || !coverImageUrl) && tiktokLink && tiktokLink.includes('tiktok.com')) {
      console.log('Downloading video and thumbnail from ssstik.io');
      const headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Referer': 'https://ssstik.io/en'
      };

      // Get tt token from main page
      const pageRes = await fetch('https://ssstik.io/en', { headers });
      const pageText = await pageRes.text();
      const $ = cheerio.load(pageText);
      const tt = $('input[name="tt"]').val();
      console.log('TT token:', tt);

      const data = new URLSearchParams();
      data.append('id', tiktokLink);
      data.append('locale', 'en');
      data.append('tt', tt || '0');

      const res2 = await fetch('https://ssstik.io/abc.php?rand=' + Math.random(), {
        method: 'POST',
        headers,
        body: data
      });
      const text2 = await res2.text();
      console.log('ssstik response:', text2);

      const $2 = cheerio.load(text2);
      sourceVideoUrl = $2('a.pure-button').attr('href'); // No-watermark link
      coverImageUrl = $2('img.result_overlay').attr('src') || $2('img').attr('src'); // Thumbnail

      if (!sourceVideoUrl || sourceVideoUrl.includes('undefined')) throw new Error('No video URL found in ssstik response');
      // Ensure .mp4 for Airtable preview
      if (!sourceVideoUrl.endsWith('.mp4')) sourceVideoUrl += '.mp4';
    }

    if (!sourceVideoUrl) throw new Error('Missing Source Video');

    // Update Source Video and Cover Image
    await base(MAIN_TABLE_NAME).update(recordId, {
      'Source Video': [{ url: sourceVideoUrl }],
      'Cover Image': coverImageUrl ? [{ url: coverImageUrl }] : []
    });

    // Use AI Character if available, fallback to coverImageUrl
    const faceImageUrl = aiCharacterUrl || coverImageUrl;
    if (!faceImageUrl) throw new Error('Missing AI Character or Cover Image for face swap');

    // Generate faces with Seedream v4.5 on Wavespeed
    console.log('Generating images with Seedream v4.5 on Wavespeed');
    const seedreamUuid = 'bytedance/seedream-v4.5/edit';
    const seedreamUrl = `https://api.wavespeed.ai/api/v3/${seedreamUuid}`;
    const seedreamData = {
      images: [faceImageUrl],
      prompt: 'high quality portrait, detailed face, realistic skin, sharp eyes',
      width: 1728,
      height: 2304,
      wait: true
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
    const generatedImages = (seedreamJson.output || []).map(url => ({ url }));
    if (generatedImages.length === 0) throw new Error('No generated images from Seedream');

    // Update Generated Images
    await base(MAIN_TABLE_NAME).update(recordId, { 'Generated Images': generatedImages });

    // Animate/face swap with Wan 2.2 Animate on Wavespeed
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
    const outputVideoUrl = wanJson.output_video_url;

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
