const sql = require('mssql'); 

module.exports = async function (context, req) {
    context.log('JavaScript HTTP trigger function processed a request.');

    const sqlConfig = {
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        server: process.env.DB_SERVER,
        database: process.env.DB_NAME,
        options: {
            encrypt: true,
            trustServerCertificate: false // Set to true for local development, false for production
        }
    };

    try {
        await sql.connect(sqlConfig);

        // req.body contains arrays for tescoPurchases, tescoWeeklyPurchases and tescoProducts
        const { tescoPurchases, tescoWeeklyPurchases, tescoProducts } = req.body;

        // Loop for tescoPurchases - Using MERGE to prevent duplicates based on hash
        if (tescoPurchases) {
            for (const item of tescoPurchases) {

                context.log('Inserting data:', item);

                await sql.query`
                    MERGE INTO tescoPurchases AS target
                    USING (SELECT ${item.hash} AS hash) AS source
                    ON target.hash = source.hash
                    WHEN NOT MATCHED THEN
                        INSERT (hash, date, storeName, storeId, storeFormat, purchaseType, basketValueGross, basketValueNet, overallBasketSavings, totalItems)
                        VALUES (${item.hash}, ${item.date}, ${item.storeName}, ${item.storeId}, ${item.storeFormat}, ${item.purchaseType}, ${item.basketValueGross}, ${item.basketValueNet}, ${item.overallBasketSavings}, ${item.totalItems});
                `;
            }
        }

        // Loop for tescoWeeklyPurchases - Direct insertion as 'submission' is not unique
        if (tescoWeeklyPurchases) {
            for (const item of tescoWeeklyPurchases) {

                context.log('Inserting data:', item);

                await sql.query`INSERT INTO tescoWeeklyPurchases (submission, weekCommencing, totalBasketValueGross, totalBasketValueNet, totalOverallBasketSavings, totalItems, outcode) VALUES (${item.submission}, ${item.weekCommencing}, ${item.totalBasketValueGross}, ${item.totalBasketValueNet}, ${item.totalOverallBasketSavings}, ${item.totalItems}, ${item.outcode})`;
            }
        }

        // Loop for tescoProducts - Using MERGE to prevent duplicates based on hash
        if (tescoProducts) {
            for (const item of tescoProducts) {
                context.log('Inserting data:', item);

                await sql.query`
                    MERGE INTO tescoProducts AS target
                    USING (VALUES (${item.hash})) AS source (hash)
                    ON target.hash = source.hash
                    WHEN NOT MATCHED THEN
                        INSERT (hash, date, name, price, storeId, storeName, storeFormat)
                        VALUES (${item.hash}, ${item.date}, ${item.name}, ${item.price}, ${item.storeId}, ${item.storeName}, ${item.storeFormat});
                `;
            }
        }

    context.res = {
        body: JSON.stringify({
            message: "Data processed successfully"
        })
    };
    } catch (err) {
        context.log.error(`SQL error: ${err}`);
        context.res = {
            status: 500,
            body: JSON.stringify({
                message: "An error occurred while processing data."
            })
        };
    }
}