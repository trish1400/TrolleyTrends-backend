const sql = require('mssql');
const { BlobServiceClient } = require('@azure/storage-blob');

// Configure the connection pool outside the function scope
const sqlConfig = {
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    server: process.env.DB_SERVER,
    database: process.env.DB_NAME,
    options: {
        encrypt: true,
        trustServerCertificate: false, // Set to true for local development, false for production
        connectTimeout: 30000
    },
    pool: {
        min: 0,
        max: 10,
        idleTimeoutMillis: 30000
    }
};

const poolPromise = new sql.ConnectionPool(sqlConfig).connect();

module.exports = async function (context, myBlob) {
    const timestamp = context.bindingData.timestamp;
    const blobName = `tescoWeeklyPurchases-${timestamp}`;
    const debug = process.env.DEBUG === 'true'; // Check if debugging is enabled

    if (debug) {
        context.log(`Blob ${blobName} processing started.`);
    }

    try {
        const pool = await poolPromise;
        if (debug) {
            context.log('Connected to SQL database successfully.');
        }

        const tescoWeeklyPurchases = JSON.parse(myBlob.toString());

        // Loop for tescoWeeklyPurchases - Direct insertion as 'submission' is not unique
        if (tescoWeeklyPurchases) {
            for (const item of tescoWeeklyPurchases) {
                await pool.request().query`INSERT INTO tescoWeeklyPurchases (submission, weekCommencing, totalBasketValueGross, totalBasketValueNet, totalOverallBasketSavings, totalItems, outcode) VALUES (${item.submission}, ${item.weekCommencing}, ${item.totalBasketValueGross}, ${item.totalBasketValueNet}, ${item.totalOverallBasketSavings}, ${item.totalItems}, ${item.outcode})`;
            }
        }

        if (debug) {
            context.log(`Blob ${blobName} processing successful.`);
        }

        const blobServiceClient = BlobServiceClient.fromConnectionString(process.env.AzureWebJobsStorage);
        const containerClient = blobServiceClient.getContainerClient("trolleytrends-tesco-anon");
        const blobClient = containerClient.getBlobClient(blobName);

        // Delete the blob after processing
        await blobClient.delete();
        if (debug) {
            context.log(`Blob ${blobName} deleted successfully`);
        }

    } catch (err) {
        context.log.error(`SQL error for Blob ${blobName}: ${err}`);
        throw err; // Rethrow the error to mark the function execution as failed
    } finally {
        // No need to close the pool, it's managed by the runtime environment
    }
};
