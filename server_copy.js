import express from 'express';
import cors from 'cors';
import multer from 'multer';
import sqlite3 from 'sqlite3';
import { createTableAndImportCSV } from './databaseSetup.js';

const app = express();
const PORT = 3000;
const upload = multer({ dest: 'uploads/' }); 

app.use(cors());
app.use(express.json());

// Adjusted the database path
const db = new (sqlite3.verbose()).Database('insurance_policies.db', sqlite3.OPEN_READWRITE, (err) => {
    if (err) {
        console.error('Error opening database', err.message);
    } else {
        console.log('Connected to the insurance policies database.');
    }
});

app.get('/insurance_policies', (req, res) => {
    db.all("SELECT * FROM insurance_policies", [], (err, rows) => {
        if (err) {
            res.status(400).json({ "error": err.message });
            return;
        }
        res.json({ "message": "success", "data": rows });
    });
});

app.post('/upload-csv', upload.single('file'), (req, res) => {
    if (!req.file) {
        return res.status(400).send('No file uploaded.');
    }
    const csvFilePath = req.file.path;
    createTableAndImportCSV('insurance_policies.db', csvFilePath)
        .then(() => res.send('File uploaded and processed successfully'))
        .catch(error => {
            console.error('Error processing file:', error);
            res.status(500).send('Failed to process file.');
        });
});

app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
