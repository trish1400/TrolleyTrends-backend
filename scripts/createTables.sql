-- Drop tables if they exist
IF OBJECT_ID('tescoPurchases', 'U') IS NOT NULL
    DROP TABLE tescoPurchases;

IF OBJECT_ID('tescoProducts', 'U') IS NOT NULL
    DROP TABLE tescoProducts;

IF OBJECT_ID('tescoWeeklyPurchases', 'U') IS NOT NULL
    DROP TABLE tescoWeeklyPurchases;

-- Recreate tables with updated schema
CREATE TABLE tescoPurchases (
    hash NVARCHAR(64) PRIMARY KEY,
    date DATE,
    storeName NVARCHAR(1024),
    storeId NVARCHAR(255),
    storeFormat NVARCHAR(255),
    purchaseType NVARCHAR(255),
    basketValueGross DECIMAL(10, 2),
    basketValueNet DECIMAL(10, 2),
    overallBasketSavings DECIMAL(10, 2),
    totalItems INT,

);

CREATE TABLE tescoProducts (
    hash NVARCHAR(64) PRIMARY KEY,
    date DATE,
    name NVARCHAR(MAX),
    price DECIMAL(10, 2),
    storeId NVARCHAR(255),
    storeName NVARCHAR(1024),
    storeFormat NVARCHAR(255)
);

CREATE TABLE tescoWeeklyPurchases (
    id INT IDENTITY(1,1) PRIMARY KEY,
    weekCommencing DATE,
    submission NVARCHAR(64),
    outcode NVARCHAR(4),
    totalBasketValueGross DECIMAL(10, 2),
    totalBasketValueNet DECIMAL(10, 2),
    totalOverallBasketSavings DECIMAL(10, 2),
    totalItems INT
);
