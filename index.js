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
  const AIRTABLE_BASE_ID = 'app3cHH00xp68kQQR'; // From your config
  const MAIN_TABLE_NAME = 'Generation'; // Actual table name
  const CONFIG_TABLE_NAME = 'Configuration'; // Put back to fetch other keys

  if (!AIRTABLE_API_KEY) return res.status(500).send('Missing AIRTABLE_API_KEY env var');

  const base = new Airtable({ apiKey: AIRTABLE_API_KEY }).base(AIRTABLE_BASE_ID);

  try {
    console.log('Fetching config from table:', CONFIG_TABLE_NAME);
    const configRecords = await base(CONFIG_TABLE_NAME).select({ view: 'Grid view' }).firstPage(); // Added view if needed
    if (!configRecords.length) throw new Error('No config record found');
    const config = configRecords[0].fields;

    const WAVESPEED_API_KEY = config['Wavespeed API Key'];
    const APIFY_API_KEY = config['Apify API Token'];
    const FAL_AI_API_KEY = config['FAL.ai API Key']; // Optional

    if (!WAVESPEED_API_KEY) throw new Error('Missing Wavespeed API Key in config');
    // APIFY_API_KEY is optional but checked later if needed

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
        // Removed 'format: "mp4"' as it's not in the standard input schema; adjust if your actor requires it
      };
      const apifyUrl = `https://api.apify.com/v2/acts/apify~tiktok-scraper/run-sync-get-dataset-items?token=${APIFY_API_KEY}`;
      const apifyRes = await fetch(apifyUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(apifyData)
      });
      if (!apifyRes.ok) throw new Error(`Apify error: ${apifyRes.statusText}`);
      const apifyJson = await apifyRes.json(); // Should be array of dataset items
      // Assuming the first item has the video URL; adjust field name based on actual output (e.g., 'playUrl', 'videoUrl', 'downloadAddr', or 'videoUrlNoWatermark')
      sourceVideoUrl = apifyJson[0]?.videoUrl || apifyJson[0]?.playUrl || apifyJson[0]?.downloadAddr;
      if (!sourceVideoUrl) throw new Error('No video URL found in Apify response');
    }

    if (!sourceVideoUrl) throw new Error('Missing Source Video');

    // Image gen... (using FAL_AI_API_KEY if needed)
    // Video gen... (using WAVESPEED_API_KEY)
    // Success update...
    // For example:
    // await base(MAIN_TABLE_NAME).update(recordId, { Status: 'Complete', 'Output Video': [{ url: generatedVideoUrl }], Generate: false });

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
