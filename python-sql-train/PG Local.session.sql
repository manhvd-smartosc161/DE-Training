SELECT DATE_TRUNC('month', o.occurred_at) ord_date, SUM(o.gloss_amt_usd) tot_spent, MAX(SUM(O.gloss_amt_usd))
FROM orders o 
JOIN accounts a
ON a.id = o.account_id
WHERE a.name = 'Walmart'
GROUP BY 1
-- ORDER BY 2 DESC
-- LIMIT 1;

-- SELECT DATE_PART('month', occurred_at) ord_month, SUM(total_amt_usd) total_spent
-- FROM orders
-- WHERE occurred_at BETWEEN '2014-01-01' AND '2017-01-01'
-- GROUP BY 1
-- ORDER BY 2 DESC;

-- SELECT COUNT(*) as total_sales_reps, a.sales_rep_id, sales_rep.name
-- FROM sales_reps as sales_rep
-- JOIN accounts as a
-- ON sales_rep.id = a.sales_rep_id
-- GROUP BY sales_rep_id, sales_rep.name
-- HAVING COUNT(a.id) > 5
-- ORDER BY total_sales_reps ASC;

-- SELECT s.id, s.name, COUNT(*) num_accounts
-- FROM accounts a
-- JOIN sales_reps s
-- ON s.id = a.sales_rep_id
-- GROUP BY s.id, s.name
-- HAVING COUNT(*) > 5
-- ORDER BY num_accounts;

-- SELECT account_id, SUM(total_amt_usd) as total_sales
-- FROM orders
-- GROUP BY 1
-- ORDER BY 2 DESC
-- SELECT DISTINCT id, name
-- FROM accounts;

-- SELECT s.name, w.channel, COUNT(*) num_events
-- FROM accounts a
-- JOIN web_events w
-- ON a.id = w.account_id
-- JOIN sales_reps s
-- ON s.id = a.sales_rep_id
-- GROUP BY s.name, w.channel
-- ORDER BY num_events DESC;

-- SELECT 
--     account_id,
--     channel,
--     COUNT(id) as total_events
-- FROM web_events
-- GROUP BY account_id, channel
-- ORDER BY account_id, total_events DESC;

-- SELECT r.name, COUNT(*) as total_sales_reps
-- FROM region as r 
-- JOIN sales_reps as sales_rep
-- ON r.id = sales_rep.region_id
-- GROUP BY r.name
-- ORDER BY total_sales_reps;



-- SELECT a.name, MIN(total_amt_usd) smallest_order
-- FROM accounts a
-- JOIN orders o
-- ON a.id = o.account_id
-- GROUP BY a.name
-- ORDER BY smallest_order;

-- SELECT 
--     account_id, 
--     SUM(standard_qty) as standard_total, 
--     SUM(gloss_qty) as gloss_total, 
--     SUM(poster_qty) as poster_total
-- FROM orders
-- GROUP BY account_id
-- ORDER BY account_id;

-- SELECT MIN(occurred_at) as earliest_order
-- FROM orders;

-- SELECT occurred_at
-- FROM orders
-- ORDER BY occurred_at ASC
-- LIMIT 1;

-- SELECT 
--     AVG(standard_qty) as avg_standard,
--     AVG(gloss_qty) as avg_gloss,
--     AVG(poster_qty) as avg_poster
-- FROM orders;

-- SELECT 
--     MIN(standard_qty) AS min_standard,
--     MIN(gloss_qty) AS min_gloss,
--     MIN(poster_qty) AS min_poster,
--     MAX(standard_qty) AS max_standard,
--     MAX(gloss_qty) AS max_gloss,
--     MAX(poster_qty) AS max_poster
-- FROM 
--     orders;

-- SELECT 
--     SUM(standard_qty) as standard_total, 
--     SUM(gloss_qty) as gloss_total, 
--     SUM(poster_qty) as poster_total
-- FROM orders;

-- SELECT *
-- FROM accounts
-- WHERE primary_poc IS NOT NULL;

-- SELECT count(*) as total_orders
-- FROM orders
-- WHERE occurred_at >= '2016-12-01' AND occurred_at <= '2017-01-01'

-- SELECT r.name region, a.name account, 
--            o.total_amt_usd/(o.total + 0.01) unit_price
-- FROM region r
-- JOIN sales_reps s ON r.id = s.region_id
-- JOIN accounts a ON s.id = a.sales_rep_id
-- JOIN orders o ON a.id = o.account_id



-- SELECT r.name region, s.name sales_rep, a.name account
-- FROM sales_reps s
-- JOIN region r ON s.region_id = r.id
-- JOIN accounts a ON s.id = a.sales_rep_id


-- SELECT orders.*, accounts.*
-- FROM accounts
-- JOIN orders
-- ON accounts.id = orders.account_id;

-- SELECT *
-- FROM orders
-- JOIN accounts 
-- ON orders.account_id = accounts.id

-- SELECT id, account_id, occurred_at, total_amt_usd
-- FROM orders
-- ORDER BY occurred_at ASC
-- LIMIT 1000;


-- SELECT account_id, occurred_at, total_amt_usd
-- FROM orders
-- ORDER BY account_id, total_amt_usd DESC;

-- SELECT *
-- FROM orders
-- WHERE gloss_amt_usd >= 1000
-- LIMIT 5;

-- SELECT *
-- FROM orders
-- WHERE total_amt_usd < 500
-- LIMIT 10;

-- SELECT id, account_id, standard_amt_usd/standard_qty AS unit_price
-- FROM orders
-- LIMIT 10;

-- SELECT *
-- FROM accounts
-- WHERE name LIKE 'C%';

-- SELECT *
-- FROM accounts
-- WHERE name LIKE '%one%';

-- SELECT *
-- FROM accounts
-- WHERE name LIKE '%s';

-- SELECT *
-- FROM accounts
-- WHERE name IN ('Walmart', 'Apple');

-- SELECT *
-- FROM accounts
-- WHERE name NOT LIKE '%google%';

-- SELECT * 
-- FROM orders