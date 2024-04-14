const sql = require('mssql');
const { BlobServiceClient } = require('@azure/storage-blob');

// Set up a persistent connection pool
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
    const blobName = `tescoPurchases-${timestamp}`;
    const debug = process.env.DEBUG === 'true'; // Check if debugging is enabled

    if (debug) {
        context.log(`Blob ${blobName} processing started.`);
    }

    try {
        const pool = await poolPromise;
        const tescoPurchases = JSON.parse(myBlob.toString());

        // Loop for tescoPurchases - Using MERGE to prevent duplicates based on hash
        if (tescoPurchases && tescoPurchases.length > 0) {
            for (const item of tescoPurchases) {
                await pool.request().query`
                    MERGE INTO tescoPurchases AS target
                    USING (SELECT ${item.hash} AS hash) AS source
                    ON target.hash = source.hash
                    WHEN NOT MATCHED THEN
                        INSERT (hash, date, storeName, storeId, storeFormat, purchaseType, basketValueGross, basketValueNet, overallBasketSavings, totalItems)
                        VALUES (${item.hash}, ${item.date}, ${item.storeName}, ${item.storeId}, ${item.storeFormat}, ${item.purchaseType}, ${item.basketValueGross}, ${item.basketValueNet}, ${item.overallBasketSavings}, ${item.totalItems});
                `;
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
    }
};
