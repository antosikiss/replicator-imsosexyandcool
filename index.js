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
  const AIRTABLE_BASE_ID = 'app3cHH00xp68kQQR';  // From your config
  const WAVESPEED_API_KEY = process.env.WAVESPEED_API_KEY;  // From config
  const APIFY_API_KEY = process.env.APIFY_API_KEY;  // For video download
  const FAL_AI_API_KEY = process.env.FAL_AI_API_KEY;  // Optional, if using FAL.ai alternative
  const MAIN_TABLE_NAME = 'Grid view';  // Exact from your table
  const CONFIG_TABLE_NAME = 'Configuration';

  if (!AIRTABLE_API_KEY || !WAVESPEED_API_KEY) return res.status(500).send('Missing env vars');

  const base = new Airtable({ apiKey: AIRTABLE_API_KEY }).base(AIRTABLE_BASE_ID);

  try {
    console.log('Fetching config from table:', CONFIG_TABLE_NAME);
    const configRecords = await base(CONFIG_TABLE_NAME).select().firstPage();
    if (!configRecords.length) throw new Error('No config');
    const config = configRecords[0].fields;

    console.log('Fetching record:', recordId);
    const record = await base(MAIN_TABLE_NAME).find(recordId);
    const fields = record.fields;
    if (!fields.Generate) return res.status(200).send('Generate not triggered');

    await base(MAIN_TABLE_NAME).update(recordId, { Status: 'Generating' });

    let sourceVideoUrl = fields['Source Video'] ? fields['Source Video'][0].url : null;
    const tiktokLink = fields.Link;

    if (!sourceVideoUrl && tiktokLink.includes('tiktok.com') && APIFY_API_KEY) {
      console.log('Downloading video from Apify');
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
      sourceVideoUrl = apifyJson.data.items[0].videoUrl;  // Adapt
    }
    if (!sourceVideoUrl) throw new Error('Missing Source Video');

    // Image gen...
    // Video gen...

    // Success update...
  } catch (error) {
    console.error('Error during generation:', error.message, error.stack);
    try {
      await base(MAIN_TABLE_NAME).update(recordId, { Status: 'Failed', 'Error Message': error.message, Generate: false });
    } catch (updateError) {
      console.error('Update failed:', updateError.message, updateError.stack);
    }
    res.status(500).send(error.message);
  }
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
