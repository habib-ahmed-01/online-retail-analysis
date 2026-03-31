-- =============================================================
-- analysis.sql
-- Online Retail Dataset — Full SQL Analysis
-- Database: online_retail   |   Table: online_retail_clean
-- =============================================================
-- Covers:
--   A. Basic SELECT & WHERE
--   B. GROUP BY aggregations
--   C. HAVING clause
--   D. ORDER BY
--   E. JOIN operations
--   F. Window Functions
--   G. Purchase Frequency Analysis
--   H. Basket Size Analysis
--   I. Revenue Trend Analysis
--   J. Product Analysis
--   K. Country Analysis

-- =============================================================

USE online_retail;

-- =============================================================
-- A.  BASIC SELECT & WHERE
-- =============================================================

-- A1. Preview first 10 rows
SELECT *
FROM online_retail_clean
LIMIT 10;


-- A2. All transactions from Germany
SELECT InvoiceNo, InvoiceDate, CustomerID, Description, Quantity, UnitPrice, TotalLineValue
FROM online_retail_clean
WHERE Country = 'Germany'
ORDER BY InvoiceDate DESC
LIMIT 50;


-- A3. High-value line items  (single line > £500)
SELECT InvoiceNo, CustomerID, Description, Quantity, UnitPrice, TotalLineValue, Country
FROM online_retail_clean
WHERE TotalLineValue > 500
ORDER BY TotalLineValue DESC;


-- A4. Transactions in November and December 2011
SELECT InvoiceNo, InvoiceDate, CustomerID, TotalLineValue
FROM online_retail_clean
WHERE InvoiceDate BETWEEN '2011-11-01' AND '2011-12-31'
ORDER BY InvoiceDate;


-- A5. Customers who placed orders on a Friday
SELECT DISTINCT CustomerID, Country
FROM online_retail_clean
WHERE DayOfWeek = 'Friday';


-- =============================================================
-- B.  GROUP BY AGGREGATIONS
-- =============================================================


-- B2. Revenue and transaction count by country
SELECT
    Country,
    COUNT(DISTINCT CustomerID)            AS UniqueCustomers,
    COUNT(DISTINCT InvoiceNo)             AS TotalOrders,
    ROUND(SUM(TotalLineValue), 2)         AS TotalRevenue,
    ROUND(AVG(TotalLineValue), 2)         AS AvgOrderLineValue
FROM online_retail_clean
GROUP BY Country
ORDER BY TotalRevenue DESC;


-- B3. Top 10 best-selling products by units sold
SELECT
    StockCode,
    Description,
    SUM(Quantity)                         AS TotalUnitsSold,
    COUNT(DISTINCT InvoiceNo)             AS TransactionCount,
    ROUND(SUM(TotalLineValue), 2)         AS TotalRevenue
FROM online_retail_clean
GROUP BY StockCode, Description
ORDER BY TotalUnitsSold DESC
LIMIT 10;


-- B4. Revenue by day of week
SELECT
    DayOfWeek,
    COUNT(DISTINCT InvoiceNo)             AS TotalTransactions,
    ROUND(SUM(TotalLineValue), 2)         AS TotalRevenue
FROM online_retail_clean
GROUP BY DayOfWeek
ORDER BY
    FIELD(DayOfWeek, 'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday');


-- =============================================================
-- C.  HAVING CLAUSE
-- =============================================================

-- C1. High-frequency customers  (more than 20 orders)
SELECT
    CustomerID,
    COUNT(DISTINCT InvoiceNo)             AS PurchaseFrequency,
    ROUND(SUM(TotalLineValue), 2)         AS TotalSpend
FROM online_retail_clean
GROUP BY CustomerID
HAVING PurchaseFrequency > 20
ORDER BY PurchaseFrequency DESC;


-- C2. Products with total revenue > £10,000
SELECT
    StockCode,
    Description,
    ROUND(SUM(TotalLineValue), 2)         AS TotalRevenue,
    SUM(Quantity)                         AS TotalUnits
FROM online_retail_clean
GROUP BY StockCode, Description
HAVING TotalRevenue > 10000
ORDER BY TotalRevenue DESC;


-- C3. Countries where average basket value exceeds £25
SELECT
    Country,
    ROUND(AVG(basket.BasketValue), 2)     AS AvgBasketValue,
    COUNT(DISTINCT CustomerID)            AS UniqueCustomers
FROM online_retail_clean
JOIN (
    SELECT InvoiceNo, ROUND(SUM(TotalLineValue), 2) AS BasketValue
    FROM online_retail_clean
    GROUP BY InvoiceNo
) AS basket USING (InvoiceNo)
GROUP BY Country
HAVING AvgBasketValue > 25
ORDER BY AvgBasketValue DESC;


-- =============================================================
-- D.  ORDER BY
-- =============================================================

-- D1. Top 10 customers by total lifetime revenue
SELECT
    CustomerID,
    Country,
    COUNT(DISTINCT InvoiceNo)             AS TotalOrders,
    ROUND(SUM(TotalLineValue), 2)         AS LifetimeValue
FROM online_retail_clean
GROUP BY CustomerID, Country
ORDER BY LifetimeValue DESC
LIMIT 10;


-- D2. Bottom 10 products by revenue (lowest performers)
SELECT
    StockCode,
    Description,
    ROUND(SUM(TotalLineValue), 2)         AS TotalRevenue
FROM online_retail_clean
GROUP BY StockCode, Description
ORDER BY TotalRevenue ASC
LIMIT 10;


-- =============================================================
-- E.  JOIN OPERATIONS
-- =============================================================

-- E1. Customer summary joined with basket-level stats
--     (self-join pattern: aggregate invoice baskets then join back)
SELECT
    c.CustomerID,
    c.Country,
    c.TotalOrders,
    c.LifetimeValue,
    ROUND(b.AvgBasketValue, 2)            AS AvgBasketValue,
    ROUND(b.AvgBasketQty,   2)            AS AvgBasketQty
FROM (
    -- Customer-level aggregation
    SELECT
        CustomerID,
        Country,
        COUNT(DISTINCT InvoiceNo)         AS TotalOrders,
        ROUND(SUM(TotalLineValue), 2)     AS LifetimeValue
    FROM online_retail_clean
    GROUP BY CustomerID, Country
) AS c
JOIN (
    -- Basket-level averages per customer
    SELECT
        CustomerID,
        AVG(BasketValue)                  AS AvgBasketValue,
        AVG(BasketQty)                    AS AvgBasketQty
    FROM (
        SELECT
            CustomerID,
            InvoiceNo,
            SUM(TotalLineValue)           AS BasketValue,
            SUM(Quantity)                 AS BasketQty
        FROM online_retail_clean
        GROUP BY CustomerID, InvoiceNo
    ) AS invoice_agg
    GROUP BY CustomerID
) AS b ON c.CustomerID = b.CustomerID
ORDER BY c.LifetimeValue DESC
LIMIT 20;



-- =============================================================
-- F.  WINDOW FUNCTIONS
-- =============================================================

-- F1. Running cumulative revenue by month
SELECT
    Year,
    Month,
    MonthName,
    ROUND(SUM(TotalLineValue), 2)         AS MonthlyRevenue,
    ROUND(
        SUM(SUM(TotalLineValue))
        OVER (ORDER BY Year, Month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    2)                                    AS CumulativeRevenue
FROM online_retail_clean
GROUP BY Year, Month, MonthName
ORDER BY Year, Month;


-- F2. Rank customers by revenue within each country
SELECT
    CustomerID,
    Country,
    ROUND(SUM(TotalLineValue), 2)         AS TotalRevenue,
    RANK() OVER (
        PARTITION BY Country
        ORDER BY SUM(TotalLineValue) DESC
    )                                     AS RankInCountry
FROM online_retail_clean
GROUP BY CustomerID, Country
ORDER BY Country, RankInCountry
LIMIT 50;




-- =============================================================
-- G.  PURCHASE FREQUENCY ANALYSIS
-- =============================================================

-- G1. Purchase frequency per customer (all customers)
SELECT
    CustomerID,
    Country,
    COUNT(DISTINCT InvoiceNo)             AS PurchaseFrequency,
    MIN(InvoiceDate)                      AS FirstPurchase,
    MAX(InvoiceDate)                      AS LastPurchase,
    DATEDIFF(MAX(InvoiceDate), MIN(InvoiceDate)) AS TenureDays
FROM online_retail_clean
GROUP BY CustomerID, Country
ORDER BY PurchaseFrequency DESC;



-- =============================================================
-- H.  BASKET SIZE ANALYSIS
-- =============================================================

-- H1. Basket size (units + value) per invoice
SELECT
    InvoiceNo,
    CustomerID,
    InvoiceDate,
    Country,
    SUM(Quantity)                         AS BasketUnits,
    COUNT(DISTINCT StockCode)             AS UniqueProducts,
    ROUND(SUM(TotalLineValue), 2)         AS BasketValue
FROM online_retail_clean
GROUP BY InvoiceNo, CustomerID, InvoiceDate, Country
ORDER BY BasketValue DESC
LIMIT 20;


-- H2. Average basket stats per customer
SELECT
    CustomerID,
    COUNT(DISTINCT InvoiceNo)             AS TotalOrders,
    ROUND(AVG(BasketValue), 2)            AS AvgBasketValue,
    ROUND(AVG(BasketUnits), 2)            AS AvgBasketUnits,
    ROUND(MAX(BasketValue), 2)            AS MaxBasketValue,
    ROUND(MIN(BasketValue), 2)            AS MinBasketValue
FROM (
    SELECT
        CustomerID,
        InvoiceNo,
        SUM(Quantity)             AS BasketUnits,
        SUM(TotalLineValue)       AS BasketValue
    FROM online_retail_clean
    GROUP BY CustomerID, InvoiceNo
) AS baskets
GROUP BY CustomerID
ORDER BY AvgBasketValue DESC
LIMIT 20;


-- H3. Overall basket size summary statistics
SELECT
    COUNT(*)                              AS TotalTransactions,
    ROUND(AVG(BasketValue), 2)            AS MeanBasketValue,
    ROUND(MIN(BasketValue), 2)            AS MinBasketValue,
    ROUND(MAX(BasketValue), 2)            AS MaxBasketValue,
    ROUND(AVG(BasketUnits), 2)            AS MeanBasketUnits
FROM (
    SELECT
        InvoiceNo,
        SUM(TotalLineValue) AS BasketValue,
        SUM(Quantity)       AS BasketUnits
    FROM online_retail_clean
    GROUP BY InvoiceNo
) AS basket_summary;


-- =============================================================
-- I.  REVENUE TREND ANALYSIS
-- =============================================================

-- I1. Weekly revenue trend
SELECT
    Year,
    WeekNo,
    COUNT(DISTINCT InvoiceNo)             AS Transactions,
    ROUND(SUM(TotalLineValue), 2)         AS WeeklyRevenue
FROM online_retail_clean
GROUP BY Year, WeekNo
ORDER BY Year, WeekNo;


-- I2. Top 3 revenue months
SELECT
    Year,
    MonthName,
    ROUND(SUM(TotalLineValue), 2)         AS Revenue
FROM online_retail_clean
GROUP BY Year, Month, MonthName
ORDER BY Revenue DESC
LIMIT 3;


-- =============================================================
-- J.  PRODUCT ANALYSIS
-- =============================================================

-- J1. Products with the highest average selling price per unit
SELECT
    StockCode,
    Description,
    ROUND(AVG(UnitPrice), 2)              AS AvgUnitPrice,
    SUM(Quantity)                         AS TotalSold
FROM online_retail_clean
GROUP BY StockCode, Description
HAVING TotalSold > 100         -- filter noise from rarely sold items
ORDER BY AvgUnitPrice DESC
LIMIT 15;


-- =============================================================
-- K.  COUNTRY ANALYSIS
-- =============================================================

-- K1. Full country breakdown
SELECT
    Country,
    COUNT(DISTINCT CustomerID)            AS UniqueCustomers,
    COUNT(DISTINCT InvoiceNo)             AS TotalOrders,
    SUM(Quantity)                         AS TotalUnits,
    ROUND(SUM(TotalLineValue), 2)         AS TotalRevenue,
    ROUND(AVG(TotalLineValue), 2)         AS AvgLineValue
FROM online_retail_clean
GROUP BY Country
ORDER BY TotalRevenue DESC;


-- K2. Revenue share % by country (using window function)
SELECT
    Country,
    ROUND(SUM(TotalLineValue), 2)         AS Revenue,
    ROUND(
        SUM(TotalLineValue) * 100.0
        / SUM(SUM(TotalLineValue)) OVER (),
    2)                                    AS RevenueSharePct
FROM online_retail_clean
GROUP BY Country
ORDER BY Revenue DESC
LIMIT 10;
