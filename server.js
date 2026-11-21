const express = require('express');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;

const DATA_DIR = path.join(__dirname, 'data');
const BACKUPS_DIR = path.join(DATA_DIR, 'backups');
const DB_FILE = path.join(DATA_DIR, 'dat-database.json');

// Ensure data directories exist
[DATA_DIR, BACKUPS_DIR].forEach(dir => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Serve static files from current directory
app.use(express.static(__dirname));

// API Sync: GET
app.get('/api/sync', (req, res) => {
  try {
    if (fs.existsSync(DB_FILE)) {
      const content = fs.readFileSync(DB_FILE, 'utf8');
      return res.json({ success: true, data: JSON.parse(content) });
    }
    return res.json({ success: true, data: null });
  } catch (err) {
    console.error('Error reading dat database:', err);
    return res.status(500).json({ success: false, error: err.message });
  }
});

// API Sync: POST
app.post('/api/sync', (req, res) => {
  try {
    const payload = req.body;
    if (!payload || typeof payload !== 'object') {
      return res.status(400).json({ success: false, error: 'Invalid payload' });
    }

    fs.writeFileSync(DB_FILE, JSON.stringify(payload, null, 2), 'utf8');

    // Create periodic timestamped backup (max 1 backup per 10 mins)
    const backupFile = path.join(BACKUPS_DIR, `dat-backup-${new Date().toISOString().slice(0, 13)}.json`);
    if (!fs.existsSync(backupFile)) {
      fs.writeFileSync(backupFile, JSON.stringify(payload, null, 2), 'utf8');
    }

    return res.json({ success: true, timestamp: new Date().toISOString() });
  } catch (err) {
    console.error('Error saving dat database:', err);
    return res.status(500).json({ success: false, error: err.message });
  }
});

// Route all other requests to index.html
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

app.listen(PORT, () => {
  console.log(`May Harvest Disciplinary Action Tracker running on port ${PORT}`);
});
