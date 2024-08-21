require('dotenv').config();
const express = require('express');
const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');
const Papa = require('papaparse');
const axios = require('axios');
const xlsx = require('xlsx');

const app = express();
const port = process.env.PORT || 3000;

const ISBNDB_API_KEY = process.env.ISBNDB_API_KEY;
const ISBNDB_BASE_URL = 'https://api2.isbndb.com';
const GOOGLE_DRIVE_FOLDER_ID = process.env.GOOGLE_DRIVE_FOLDER_ID;


// Set up Google Drive API
// const auth = new google.auth.GoogleAuth({
//   credentials: JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS),
//   scopes: ['https://www.googleapis.com/auth/drive.readonly'],
// });
// const drive = google.drive({ version: 'v3', auth });
const auth = new google.auth.GoogleAuth({
    keyFile: 'bbc-bs-28b1398a3317.json',
    scopes: ['https://www.googleapis.com/auth/drive.readonly'],
  });

// Initialize the Google Drive client
let drive;

// Function to initialize the drive client
async function initializeDriveClient() {
  const authClient = await auth.getClient();
  drive = google.drive({ version: 'v3', auth: authClient });
}

app.use(express.static('public'));

app.post('/process', async (req, res) => {
  try {
    const result = await processGoogleDriveFiles();
    const outputPath = path.join(__dirname, 'output.csv');
    exportToCSV(result, outputPath);
    res.json({ message: 'Processing complete', downloadUrl: '/download' });
  } catch (error) {
    console.error('Error processing files:', error);
    res.status(500).json({ error: 'An error occurred while processing the files' });
  }
});

app.get('/', (req, res) => {
    res.send('Google Drive File Processor is running')
})

app.get('/download', (req, res) => {
  const file = path.join(__dirname, 'output.csv');
  res.download(file);
});

// ... (previous code remains the same)
// Header mapping for different CSV formats
const headerMap = {
    'ISBN_number': 'ISBN',
    'SKU': 'ISBN',
    'Ean': 'ISBN',
    'Net quantity': 'Sales',
    'Qty': 'Sales',
    'QTY': 'Sales',
    'Count': 'Sales',
    'Units': 'Sales',
    'Items Sold': 'Sales',
    'Units Sold': 'Sales',
    'GTIN': 'ISBN',
    ' GTIN': 'ISBN',
    'ISBN ': 'ISBN',
    ' Sls' : 'Sales',
    'SALES' : 'Sales',
    'Sls' : 'Sales',
    'ISBN         ': 'ISBN',
    'Item Code': 'ISBN'
    // Add more mappings as needed
};

function normalizeHeaders(headers) {
    return headers.map(header => headerMap[header] || header);
}

function validateAndFormatISBN(isbn) {
    // Convert to string and remove any non-digit characters
    let cleanIsbn = String(isbn).replace(/[^0-9X]/gi, '');

    // Check if it's in scientific notation
    if (/e/i.test(String(isbn))) {
        console.warn(`Skipping ISBN in scientific notation: ${isbn}`);
        return null;
    }

    // Check length
    if (cleanIsbn.length !== 10 && cleanIsbn.length !== 13) {
        console.warn(`Skipping invalid ISBN length: ${isbn}`);
        return null;
    }

    // Additional checks can be added here (e.g., checksum validation)

    return cleanIsbn;
}

async function processGoogleDriveFiles() {
    console.log('Starting to process Google Drive files...');
    const files = await listFilesInFolder(GOOGLE_DRIVE_FOLDER_ID);
    console.log(`Found ${files.length} files to process.`);
    
    let bookSales = new Map();
    let isbnStores = new Map();
    let invalidIsbnCount = 0;
  
    for (const file of files) {
        try {
            console.log(`Processing file: ${file.name}`);
            const fileContent = await downloadFile(file.id);
            let parsedData;
        
            // Initialize a Set to keep track of unique ISBNs for this file
            let uniqueIsbnsInFile = new Set();      
        
            if (file.name.endsWith('.csv')) {
                parsedData = await parseCSV(fileContent);
            } else if (file.name.endsWith('.xlsx') || file.name.endsWith('.xls')) {
                parsedData = parseExcel(fileContent);
            } else {
                console.log(`Skipping unsupported file type: ${file.name}`);
                continue;
            }

            console.log(`Parsed ${parsedData.length} rows from ${file.name}`);

            parsedData.forEach(row => {
                const isbn = row['ISBN'];
                const sales = parseInt(row['Sales'], 10);

                if (isbn && !isNaN(sales)) {
                    bookSales.set(isbn, (bookSales.get(isbn) || 0) + sales);
                    isbnStores.set(isbn, (isbnStores.get(isbn) || 0) + 1);
                } else if (!isbn) {
                    invalidIsbnCount++;
                }
                // Add the ISBN to the set of unique ISBNs for this file
                uniqueIsbnsInFile.add(isbn);
            });
           
            // Update the isbnStores with the unique ISBNs found in this file
            uniqueIsbnsInFile.forEach(isbn => {
                isbnStores.set(isbn, (isbnStores.get(isbn) || 0) + 1);
            });
        } catch (error) {
        console.error(`Error processing file ${file.name}: ${error}`);
      }
    }
  
    console.log(`Processed data for ${bookSales.size} unique ISBNs`);
    console.log(`Skipped ${invalidIsbnCount} rows with invalid ISBNs`);

    let combinedData = Array.from(bookSales).map(([isbn, sales]) => {
      return { ISBN: isbn, Sales: sales, Stores: isbnStores.get(isbn) };
    });
  
    combinedData.sort((a, b) => (b.Stores - a.Stores || b.Sales - a.Sales));
  
    let top200 = combinedData.slice(0, 200);
    let isbndbFail = 0;
  
    for (let book of top200) {
      console.log("Fetching book info for ISBN:", book.ISBN);
      const bookInfo = await searchBookInfo(book.ISBN);
      if (bookInfo) {
        book.Title = bookInfo.title || 'Unknown';
        book.Authors = bookInfo.authors ? bookInfo.authors.join(', ') : 'Unknown';
        book.Publisher = bookInfo.publisher || 'Unknown';
        book.Categories = categorizeBook(bookInfo);
        book.Subjects = bookInfo.subjects ? bookInfo.subjects.join(', ') : 'Unknown';
        book.Description = bookInfo.synopsis || 'Unknown';
      } else {
        isbndbFail++;
      }
    }
  
    console.log('ISBNdb lookup failures:', isbndbFail);
    return top200;
}
  
async function listFilesInFolder(folderId) {
    const res = await drive.files.list({
      q: `'${folderId}' in parents`,
      fields: 'files(id, name, mimeType)',
    });
    return res.data.files;
}
  
async function downloadFile(fileId) {
    const res = await drive.files.get({ fileId, alt: 'media' }, { responseType: 'arraybuffer' });
    return Buffer.from(res.data);
}
  
function parseCSV(fileContent) {
    return new Promise((resolve, reject) => {
        Papa.parse(fileContent.toString(), {
            header: true,
            transformHeader: function(header) {
                return normalizeHeaders([header])[0];
            },
            transform: function(value, field) {
                if (field === 'ISBN') {
                    return validateAndFormatISBN(value);
                }
                return value;
            },
            complete: (results) => {
                console.log('CSV Parsing complete. Rows:', results.data.length);
                console.log('Fields after normalization:', Object.keys(results.data[0]));
                resolve(results.data);
            },
            error: (error) => {
                console.error('Error parsing CSV:', error.message);
                reject(error);
            }
        });
    });
}
  
function parseExcel(fileContent) {
    const workbook = xlsx.read(fileContent, { type: 'buffer' });
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const jsonData = xlsx.utils.sheet_to_json(worksheet, { header: 1 });
    
    if (jsonData.length > 0) {
        const headers = normalizeHeaders(jsonData[0]);
        const data = jsonData.slice(1).map(row => {
            let obj = {};
            headers.forEach((header, index) => {
                if (header === 'ISBN') {
                    obj[header] = validateAndFormatISBN(row[index]);
                } else {
                    obj[header] = row[index];
                }
            });
            return obj;
        });
        console.log('Excel Parsing complete. Rows:', data.length);
        console.log('Fields after normalization:', headers);
        return data;
    }
    return [];
}
  
async function searchBookInfo(isbn) {
    const url = `${ISBNDB_BASE_URL}/book/${isbn}`;
    try {
      const response = await axios.get(url, {
        headers: {
          'Authorization': ISBNDB_API_KEY,
          'Content-Type': 'application/json'
        }
      });
      return response.data.book || null;
    } catch (error) {
      console.error(`Error fetching book info for ISBN ${isbn}:`, error);
      return null;
    }
}
  
function categorizeBook(bookInfo) {
    if (!Array.isArray(bookInfo.subjects) || bookInfo.subjects.length === 0) {
      return "Unknown";
    }
    if (bookInfo.subjects.includes("Children's Books")) {
      return "Children's";
    } else if (bookInfo.subjects.includes("Teen & Young Adult")) {
      return "Young Adult";
    } else if (bookInfo.subjects.includes("Genre Fiction")) {
      return "Fiction";
    } else {
      return "Non-Fiction";
    }
}
  
  // ... (rest of the server code)

function exportToCSV(data, outputPath) {
  const csv = Papa.unparse(data);
  fs.writeFileSync(outputPath, csv);
}

initializeDriveClient().then(() => {
    app.listen(port, () => {
      console.log(`Server running on port ${port}`);
    });
}).catch(error => {
    console.error('Failed to initialize Google Drive client:', error);
});