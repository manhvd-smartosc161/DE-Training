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