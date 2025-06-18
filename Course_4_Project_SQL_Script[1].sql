-- Task 1

WITH monthly_sales AS (
    SELECT 
        Branch,
        DATE_FORMAT(STR_TO_DATE(Date, '%d/%m/%Y'), '%Y-%m') AS month,
        SUM(Total) AS monthly_total
    FROM `walmartsales dataset - walmartsales`
    GROUP BY Branch, DATE_FORMAT(STR_TO_DATE(Date, '%d/%m/%Y'), '%Y-%m')
),
growth_calc AS (
    SELECT 
        m1.Branch,
        m1.month,
        m1.monthly_total,
        LAG(m1.monthly_total) OVER (PARTITION BY m1.Branch ORDER BY m1.month) AS prev_month_total,
        (m1.monthly_total - LAG(m1.monthly_total) OVER (PARTITION BY m1.Branch ORDER BY m1.month)) 
            / NULLIF(LAG(m1.monthly_total) OVER (PARTITION BY m1.Branch ORDER BY m1.month), 0) 
            AS sales_growth
    FROM monthly_sales m1
),
avg_growth_rate AS (
    SELECT 
        Branch,
        AVG(sales_growth) AS avg_monthly_growth
    FROM growth_calc
    WHERE sales_growth IS NOT NULL
    GROUP BY Branch
)
SELECT 
    Branch,
    ROUND(avg_monthly_growth * 100, 2) AS avg_growth_percentage
FROM avg_growth_rate
ORDER BY avg_monthly_growth DESC;


-- Task 2

WITH product_profits AS (
    SELECT 
        Branch,
        `Product line`,
        ROUND(SUM(`gross income`) - SUM(cogs), 2) AS total_profit
    FROM `walmartsales dataset - walmartsales`
    GROUP BY Branch, `Product line`
)
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY Branch ORDER BY total_profit DESC) AS rn
    FROM product_profits
) ranked
WHERE rn = 1;


-- Task 3

WITH customer_avg_spending AS (
    SELECT 
        `Customer ID`,
        ROUND(AVG(Total), 2) AS avg_spending
    FROM `walmartsales dataset - walmartsales`
    GROUP BY `Customer ID`
),
segmented_customers AS (
    SELECT 
        `Customer ID`,
        avg_spending,
        NTILE(3) OVER (ORDER BY avg_spending DESC) AS tier
    FROM customer_avg_spending
)
SELECT 
    `Customer ID`,
    avg_spending,
    CASE 
        WHEN tier = 1 THEN 'High'
        WHEN tier = 2 THEN 'Medium'
        ELSE 'Low'
    END AS spending_segment
FROM segmented_customers
ORDER BY avg_spending DESC;


-- Task 4

WITH product_line_stats AS (
    SELECT 
        `Product line`,
        AVG(Total) AS avg_total,
        STDDEV(Total) AS std_total
    FROM `walmartsales dataset - walmartsales`
    GROUP BY `Product line`
),
anomalies AS (
    SELECT 
        s.`Invoice ID`,
        s.Branch,
        s.`Product line`,
        s.Total,
        p.avg_total,
        p.std_total,
        CASE 
            WHEN s.Total > p.avg_total + 2 * p.std_total THEN 'High Anomaly'
            WHEN s.Total < p.avg_total - 2 * p.std_total THEN 'Low Anomaly'
            ELSE NULL
        END AS anomaly_type
    FROM `walmartsales dataset - walmartsales` s
    JOIN product_line_stats p ON s.`Product line` = p.`Product line`
)
SELECT 
    `Invoice ID`,
    Branch,
    `Product line`,
    Total,
    ROUND(avg_total, 2) AS avg_total,
    ROUND(std_total, 2) AS std_total,
    anomaly_type
FROM anomalies
WHERE anomaly_type IS NOT NULL
ORDER BY `Product line`, Total DESC;


-- Task 5

SELECT 
    p.City,
    p.Payment,
    p.payment_count
FROM (
    SELECT 
        City,
        Payment,
        COUNT(*) AS payment_count
    FROM `walmartsales dataset - walmartsales`
    GROUP BY City, Payment
) p
JOIN (
    SELECT 
        City,
        MAX(payment_count) AS max_count
    FROM (
        SELECT 
            City,
            Payment,
            COUNT(*) AS payment_count
        FROM `walmartsales dataset - walmartsales`
        GROUP BY City, Payment
    ) temp
    GROUP BY City
) max_p ON p.City = max_p.City AND p.payment_count = max_p.max_count
ORDER BY p.City;


-- Task 6

SELECT 
    DATE_FORMAT(STR_TO_DATE(Date, '%d/%m/%Y'), '%Y-%m') AS sale_month,
    Gender,
    ROUND(SUM(Total), 2) AS total_sales
FROM `walmartsales dataset - walmartsales`
WHERE STR_TO_DATE(Date, '%d/%m/%Y') IS NOT NULL
GROUP BY sale_month, Gender
ORDER BY sale_month, Gender;


-- Task 7

SELECT 
    p.`Customer type`, 
    p.`Product line`, 
    p.purchase_count
FROM (
    SELECT 
        `Customer type`,
        `Product line`,
        COUNT(*) AS purchase_count
    FROM `walmartsales dataset - walmartsales`
    GROUP BY `Customer type`, `Product line`
) p
JOIN (
    SELECT 
        `Customer type`, 
        MAX(purchase_count) AS max_count
    FROM (
        SELECT 
            `Customer type`,
            `Product line`,
            COUNT(*) AS purchase_count
        FROM `walmartsales dataset - walmartsales`
        GROUP BY `Customer type`, `Product line`
    ) sub
    GROUP BY `Customer type`
) max_p 
ON p.`Customer type` = max_p.`Customer type`
AND p.purchase_count = max_p.max_count
ORDER BY p.`Customer type`;


-- Task 8

WITH cleaned_data AS (
    SELECT 
        `Customer ID`,
        STR_TO_DATE(Date, '%d/%m/%Y') AS purchase_date
    FROM `walmartsales dataset - walmartsales`
),
repeat_within_30days AS (
    SELECT 
        a.`Customer ID`,
        a.purchase_date,
        b.purchase_date AS next_purchase_date,
        DATEDIFF(b.purchase_date, a.purchase_date) AS days_diff
    FROM cleaned_data a
    JOIN cleaned_data b
      ON a.`Customer ID` = b.`Customer ID`
     AND b.purchase_date > a.purchase_date
     AND DATEDIFF(b.purchase_date, a.purchase_date) <= 30
),
customer_repeat_counts AS (
    SELECT `Customer ID`, COUNT(*) AS repeat_purchase_count
    FROM repeat_within_30days
    GROUP BY `Customer ID`
)
SELECT `Customer ID`, repeat_purchase_count
FROM customer_repeat_counts
ORDER BY repeat_purchase_count DESC;


-- Task 9

SELECT 
    `Customer ID`,
    ROUND(SUM(`Total`), 2) AS total_revenue
FROM `walmartsales dataset - walmartsales`
GROUP BY `Customer ID`
ORDER BY total_revenue DESC
LIMIT 5;


-- Task 10

SELECT 
    DAYNAME(STR_TO_DATE(`Date`, '%d/%m/%Y')) AS day_of_week,
    ROUND(SUM(`Total`), 2) AS total_sales
FROM `walmartsales dataset - walmartsales`
WHERE STR_TO_DATE(`Date`, '%d/%m/%Y') IS NOT NULL
GROUP BY day_of_week
ORDER BY total_sales DESC;




