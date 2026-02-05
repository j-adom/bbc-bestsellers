require('dotenv').config();
const express = require('express');
const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');
const Papa = require('papaparse');
const axios = require('axios');
const xlsx = require('xlsx');

const app = express();
const port = process.env.PORT || 3001;

const ISBNDB_API_KEY = process.env.ISBNDB_API_KEY;
const ISBNDB_BASE_URL = 'https://api2.isbndb.com';
const GOOGLE_DRIVE_FOLDER_ID = process.env.GOOGLE_DRIVE_FOLDER_ID;
const GOOGLE_CREDENTIALS_FILE =
  process.env.GOOGLE_APPLICATION_CREDENTIALS ||
  process.env.GOOGLE_CREDENTIALS_FILE;
const GOOGLE_CREDENTIALS_JSON = process.env.GOOGLE_CREDENTIALS_JSON;


// Set up Google Drive API
const googleAuthOptions = {
  scopes: ['https://www.googleapis.com/auth/drive.readonly'],
};
if (GOOGLE_CREDENTIALS_JSON) {
  try {
    let jsonString = GOOGLE_CREDENTIALS_JSON.trim();

    // Check if it's base64 encoded (doesn't start with '{')
    if (!jsonString.startsWith('{')) {
      console.log('Detected base64 encoded credentials, decoding...');
      jsonString = Buffer.from(jsonString, 'base64').toString('utf-8');
    }
    // If the JSON is escaped (contains backslashes before quotes), unescape it
    else if (jsonString.startsWith('{\\')) {
      console.log('Detected escaped JSON, unescaping...');
      jsonString = JSON.parse(`"${jsonString}"`);
    }

    googleAuthOptions.credentials = JSON.parse(jsonString);
    console.log('✓ Successfully parsed Google credentials');
  } catch (error) {
    console.error('Failed to parse GOOGLE_CREDENTIALS_JSON:', error.message);
    console.error('Value length:', GOOGLE_CREDENTIALS_JSON?.length);
    throw new Error(`Invalid GOOGLE_CREDENTIALS_JSON: ${error.message}`);
  }
} else if (GOOGLE_CREDENTIALS_FILE) {
  googleAuthOptions.keyFile = GOOGLE_CREDENTIALS_FILE;
} else {
  throw new Error(
    'Missing Google credentials. Set GOOGLE_APPLICATION_CREDENTIALS (path) or GOOGLE_CREDENTIALS_JSON.'
  );
}
const auth = new google.auth.GoogleAuth(googleAuthOptions);

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
});

app.get('/download', (req, res) => {
  const file = path.join(__dirname, 'output.csv');
  res.download(file);
});

// ... (previous code remains the same)
// Header mapping for different CSV formats
const headerMap = {
    'ISBN_number': 'ISBN',
    'SKU': 'ISBN',
    'isbn': 'ISBN',
    'sku': 'ISBN',
    'Ean': 'ISBN',
    'Net quantity': 'Sales',
    'Qty': 'Sales',
    'QTY': 'Sales',
    'Quantity': 'Sales',
    'QUANTITY': 'Sales',
    'Count': 'Sales',
    'Units': 'Sales',
    'units': 'Sales',
    'Items Sold': 'Sales',
    'Units Sold': 'Sales',
    'GTIN': 'ISBN',
    'Lineitem quantity': 'Sales',
    'Lineitem sku': 'ISBN',
    'Item': 'ISBN',
    ' GTIN': 'ISBN',
    'GTIN': 'ISBN',
    'ISBN ': 'ISBN',
    ' Sls': 'Sales',
    'sales': 'Sales',
    'SALES': 'Sales',
    'Sls': 'Sales',
    'soldqty': 'Sales',
    'ISBN         ': 'ISBN',
    'Item Code': 'ISBN',
    'Net Quantity': 'Sales',
    'SOLD': 'Sales',
    'Sold': 'Sales',
    // Add more mappings as needed
};

function normalizeHeaders(headers) {
    return headers.map(header => headerMap[header] || header);
}

function validateAndFormatISBN(input) {
  if (typeof input !== 'string') {
    input = String(input);
  }

  // Convert scientific notation to full number string
  if (input.includes('e') || input.includes('E')) {
    const num = Number(input);
    if (!Number.isNaN(num)) {
      input = num.toFixed(0); // Remove decimals if any
    }
  }

  // Remove all non-digit characters
  const digitsOnly = input.replace(/\D/g, '');

  // Validate 13-digit ISBNs starting with 978 or 979
  if (/^97[89]\d{10}$/.test(digitsOnly)) {
    return digitsOnly;
  }

  // Optionally log or return null if invalid
  return null;
}

async function searchBooksInfo(isbns) {
  let headers = {
      "Content-Type": 'application/json',
      "Authorization": ISBNDB_API_KEY
  };

  const instance = axios.create({
      baseURL: 'https://api.premium.isbndb.com',
      headers: headers
  });

  try {
      const response = await instance.post('/books', { isbns });
      // console.log(response);

      // Assuming the response structure contains the books information
      if (response.data.data ) {
          return response.data.data; // Return the books data if available
      } else {
          console.error('Unexpected response structure:', response.data);
          return []; // Return an empty array if the structure is not as expected
      }
  } catch (error) {
      console.error(`Error fetching book info:`, error);
      return []; // Return an empty array on error
  }
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
                    // Add the ISBN to the set of unique ISBNs for this file
                    uniqueIsbnsInFile.add(isbn);
                } else if (!isbn) {
                    invalidIsbnCount++;
                }
                
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
    // Fetch book info for all ISBNs at once
    let top250 =  combinedData.slice(0, 250);
    let topISBNS = top250.map(obj => obj.ISBN);
    const bookInfo = await searchBooksInfo(topISBNS);

    // Track data quality metrics
    let dataQuality = {
      total: top250.length,
      foundInAPI: bookInfo.length,
      missingFromAPI: top250.length - bookInfo.length,
      missingDescriptions: 0,
      missingSubjects: 0,
      unknownCategories: 0
    };

    // Create a map of ISBNs from bookInfo for quick lookup
    const bookInfoMap = new Map(bookInfo.map(book => [book.isbn13, book]));

    // Combine fetched book info with sales and store counts
    let finalBooks = top250.map(bookData => {
      const book = bookInfoMap.get(bookData.ISBN);

      // If book not found in API, create minimal record
      if (!book) {
        console.warn(`ISBN ${bookData.ISBN} not found in ISBNdb API`);
        return {
          ISBN: bookData.ISBN,
          Sales: bookData.Sales,
          Stores: bookData.Stores,
          Title: 'Data Missing - Check ISBN',
          Authors: 'Unknown',
          Publisher: 'Unknown',
          Categories: 'Unknown',
          Description: 'Book data not found in ISBNdb',
          Binding: 'Unknown',
          Subjects: 'Unknown'
        };
      }

      const subjects = book.subjects || [];
      const subjectsString = Array.isArray(subjects) ? subjects.join(', ') : 'Unknown';
      const description = book.description || book.synopsis || 'Unknown';
      const category = categorizeBook(book);

      // Track data quality issues
      if (description === 'Unknown') dataQuality.missingDescriptions++;
      if (subjectsString === 'Unknown' || subjects.length === 0) dataQuality.missingSubjects++;
      if (category === 'Unknown') dataQuality.unknownCategories++;

      return {
        ISBN: book.isbn13,
        Sales: bookSales.get(book.isbn13) || 0,
        Stores: isbnStores.get(book.isbn13) || 0,
        Title: book.title || 'Unknown',
        Authors: book.authors ? book.authors.join(', ') : 'Unknown',
        Publisher: book.publisher || 'Unknown',
        Categories: category,
        Description: description,
        Binding: book.binding || 'Unknown',
        Subjects: subjectsString
      };
    });

    // Log data quality report
    console.log('\n=== DATA QUALITY REPORT ===');
    console.log(`Total books processed: ${dataQuality.total}`);
    console.log(`Found in ISBNdb: ${dataQuality.foundInAPI}`);
    console.log(`Missing from ISBNdb: ${dataQuality.missingFromAPI}`);
    console.log(`Missing descriptions: ${dataQuality.missingDescriptions}`);
    console.log(`Missing subjects: ${dataQuality.missingSubjects}`);
    console.log(`Unknown categories: ${dataQuality.unknownCategories}`);
    console.log('===========================\n');

    console.log('Sample book data:', finalBooks[0]);
    return finalBooks;
    // let top200 = combinedData.slice(0, 200);
    // let isbndbFail = 0;
  
    // for (let book of top200) {
    //   console.log("Fetching book info for ISBN:", book.ISBN);
    //   const bookInfo = await searchBookInfo(book.ISBN);
    //   if (bookInfo) {
    //     book.Title = bookInfo.title || 'Unknown';
    //     book.Authors = bookInfo.authors ? bookInfo.authors.join(', ') : 'Unknown';
    //     book.Binding || 'Unknown',
    //     book.Publisher = bookInfo.publisher || 'Unknown';
    //     book.Categories = categorizeBook(bookInfo);
    //     book.Subjects = bookInfo.subjects ? bookInfo.subjects.join(', ') : 'Unknown';
    //     book.Description = bookInfo.synopsis || 'Unknown';
    //   } else {
    //     isbndbFail++;
    //   }
    // }
  
    // console.log('ISBNdb lookup failures:', isbndbFail);
    // return top200;
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
    // Combine subjects, title, and other metadata for better categorization
    const subjects = bookInfo.subjects || [];
    const subjectsString = Array.isArray(subjects) ? subjects.join(', ').toLowerCase() : '';
    const title = (bookInfo.title || '').toLowerCase();
    const binding = (bookInfo.binding || '').toLowerCase();

    // Return Unknown if no data available
    if (!subjectsString && !title) {
      return "Unknown";
    }

    // Define keyword patterns for each category
    const childrenKeywords = [
      "children's books", "juvenile fiction", "juvenile nonfiction",
      "board book", "picture book", "early reader", "ages 0-", "ages 1-",
      "ages 2-", "ages 3-", "ages 4-", "ages 5-", "ages 6-", "ages 7-", "ages 8-",
      "preschool", "kindergarten", "baby", "toddler"
    ];

    const yaKeywords = [
      "teen & young adult", "young adult fiction", "ya fiction", "teen fiction",
      "juvenile fiction", "ages 12-", "ages 13-", "ages 14-", "ages 15-", "ages 16-", "ages 17-",
      "coming of age", "high school", "teenager"
    ];

    const fictionKeywords = [
      "fiction", "novel", "romance", "thriller", "mystery", "science fiction",
      "fantasy", "horror", "literary fiction", "historical fiction", "crime",
      "suspense", "adventure", "dystopian", "paranormal", "contemporary fiction",
      "genre fiction", "stories", "sagas"
    ];

    // Check Children's first (most specific)
    if (childrenKeywords.some(keyword => subjectsString.includes(keyword) || title.includes(keyword)) ||
        binding.includes("board book")) {
      return "Children's";
    }

    // Check Young Adult
    if (yaKeywords.some(keyword => subjectsString.includes(keyword) || title.includes(keyword))) {
      return "Young Adult";
    }

    // Check Fiction (must come before Non-Fiction to avoid false negatives)
    if (fictionKeywords.some(keyword => subjectsString.includes(keyword))) {
      return "Fiction";
    }

    // Default to Non-Fiction if none of the fiction/children/YA keywords match
    // Most books without "fiction" in subjects are indeed non-fiction
    return "Non-Fiction";
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
