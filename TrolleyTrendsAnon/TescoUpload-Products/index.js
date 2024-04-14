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
    const blobName = `tescoProducts-${timestamp}`;
    const debug = process.env.DEBUG === 'true'; // Check if debugging is enabled
    
    if (debug) {
        context.log(`Blob ${blobName} processing started.`);
    }

    try {
        const pool = await poolPromise;
        const tescoProducts = JSON.parse(myBlob.toString());

        for (const item of tescoProducts) {
            await pool.request().query`
                MERGE INTO tescoProducts AS target
                USING (VALUES (${item.hash})) AS source (hash)
                ON target.hash = source.hash
                WHEN NOT MATCHED THEN
                    INSERT (hash, date, name, price, storeId, storeName, storeFormat)
                    VALUES (${item.hash}, ${item.date}, ${item.name}, ${item.price}, ${item.storeId}, ${item.storeName}, ${item.storeFormat});
            `;
        }

        if (debug) {
            context.log(`Blob ${blobName} processing successful.`);
        }

        const blobServiceClient = BlobServiceClient.fromConnectionString(process.env.AzureWebJobsStorage);
        const containerClient = blobServiceClient.getContainerClient("trolleytrends-tesco-anon");
        const blobClient = containerClient.getBlobClient(blobName);

        await blobClient.delete();
        if (debug) {
            context.log(`Blob ${blobName} deleted successfully`);
        }

    } catch (err) {
        context.log.error(`SQL error for Blob ${blobName}: ${err.name}: ${err.code} => ${err.message}`);
        throw err; // Rethrow the error to mark the function execution as failed
    }
};

