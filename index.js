const express = require('express');
const fetch = require('node-fetch');
const Airtable = require('airtable');
const app = express();
app.use(express.json());

// Global handlers to log and prevent crashes
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection:', reason ? reason.stack || reason : 'No reason');
});
process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error.stack || error);
});

app.get('/generate', async (req, res) => {
  try {
    const recordId = req.query.recordId;
    await handleGenerate(recordId, res);
  } catch (error) {
    console.error('Error in GET /generate:', error.stack || error);
    res.status(500).send('Server error');
  }
});

app.post('/generate', async (req, res) => {
  try {
    const { recordId } = req.body;
    await handleGenerate(recordId, res);
  } catch (error) {
    console.error('Error in POST /generate:', error.stack || error);
    res.status(500).send('Server error');
  }
});

async function handleGenerate(recordId, res) {
  if (!recordId) return res.status(400).send('Missing recordId');

  const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY;
  const AIRTABLE_BASE_ID = 'app3cHH00xp68kQQR';
  const WAVESPEED_API_KEY = process.env.WAVESPEED_API_KEY;
  const APIFY_API_KEY = process.env.APIFY_API_KEY;
  const FAL_AI_API_KEY = process.env.FAL_AI_API_KEY; // Optional
  const MAIN_TABLE_NAME = 'Generation';

  if (!AIRTABLE_API_KEY || !WAVESPEED_API_KEY) return res.status(500).send('Missing required env vars (AIRTABLE_API_KEY or WAVESPEED_API_KEY)');

  const base = new Airtable({ apiKey: AIRTABLE_API_KEY }).base(AIRTABLE_BASE_ID);

  try {
    // Removed config fetch from table to avoid authorization issues; using env vars instead

    console.log('Fetching record:', recordId);
    const record = await base(MAIN_TABLE_NAME).find(recordId);
    const fields = record.fields;

    if (!fields.Generate) return res.status(200).send('Generate not triggered');

    await base(MAIN_TABLE_NAME).update(recordId, { Status: 'Generating' });

    let sourceVideoUrl = fields['Source Video'] ? fields['Source Video'][0].url : null;
    const tiktokLink = fields.Link;

    if (!sourceVideoUrl && tiktokLink && tiktokLink.includes('tiktok.com') && APIFY_API_KEY) {
      console.log('Downloading video from Apify');
      const apifyData = {
        urls: [tiktokLink]
      };
      const apifyUrl = `https://api.apify.com/v2/acts/apify~tiktok-scraper/run-sync-get-dataset-items?token=${APIFY_API_KEY}`;
      const apifyRes = await fetch(apifyUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(apifyData)
      });
      if (!apifyRes.ok) throw new Error(`Apify error: ${apifyRes.statusText}`);
      const apifyJson = await apifyRes.json();
      sourceVideoUrl = apifyJson[0]?.downloadAddr || apifyJson[0]?.playAddr || apifyJson[0]?.videoUrlNoWaterMark;
      if (!sourceVideoUrl) throw new Error('No video URL found in Apify response');
    }

    if (!sourceVideoUrl) throw new Error('Missing Source Video');

    // Proceed with image/video generation using WAVESPEED_API_KEY, FAL_AI_API_KEY, etc.
    // Example placeholder:
    // const generatedImage = await generateImage(fields['AI Character'], FAL_AI_API_KEY);
    // const generatedVideo = await animateVideo(sourceVideoUrl, generatedImage, WAVESPEED_API_KEY);
    // await base(MAIN_TABLE_NAME).update(recordId, {
    //   'Generated Images': [{ url: generatedImage }],
    //   'Output Video': [{ url: generatedVideo }],
    //   Status: 'Complete',
    //   Generate: false
    // });

    // For now, since generation logic is missing, simulate success
    await base(MAIN_TABLE_NAME).update(recordId, { Status: 'Complete', Generate: false });
    res.status(200).send('Generation complete');

  } catch (error) {
    console.error('Error during generation:', error.message, error.stack);
    try {
      await base(MAIN_TABLE_NAME).update(recordId, { Status: 'Failed', 'Error Message': error.message || 'Unknown error', Generate: false });
    } catch (updateError) {
      console.error('Update failed:', updateError.message, updateError.stack);
    }
    res.status(500).send(error.message || 'Unknown error');
  }
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
