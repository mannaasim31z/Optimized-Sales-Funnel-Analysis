-- Data cleaaning
ALTER TABLE dim_campaigns
MODIFY COLUMN start_date DATE;

ALTER TABLE dim_campaigns
MODIFY COLUMN end_date DATE;

ALTER TABLE dim_date
MODIFY COLUMN date DATE;

UPDATE dim_date
SET date=DATE_FORMAT(Date,'%Y-%m-%d');

ALTER TABLE dim_users
MODIFY COLUMN date_of_birth DATE;

ALTER TABLE fact_action
MODIFY COLUMN action_timestamp TIMESTAMP;

ALTER TABLE fact_orders 
MODIFY COLUMN date DATE;

UPDATE fact_orders
SET date=DATE_FORMAT(Date,'%Y-%m-%d');

-----------------------------------------------------------------------------------------------------------------------------------
-- 1. Overall Sales Funnel
WITH visits AS (
SELECT DISTINCT(user_id)
FROM fact_action 
WHERE action_type='website_visit'),
clicks AS (
SELECT DISTINCT(v.user_id)
FROM visits v 
JOIN fact_action a 
ON v.user_id=a.user_id
WHERE action_type='click'),
cart AS (
SELECT DISTINCT(c.user_id)
FROM clicks c 
JOIN fact_action a 
ON c.user_id=a.user_id
WHERE action_type='add_to_cart'),
orders AS (
SELECT DISTINCT(c.user_id)
FROM cart c 
JOIN fact_action a 
ON c.user_id=a.user_id
WHERE action_type='order'),
master_data AS (
SELECT 'website_visit' AS action_type,COUNT(*) AS users FROM visits
UNION
SELECT 'click' AS action_type,COUNT(*) AS users FROM clicks
UNION
SELECT 'order_cart' AS action_type,COUNT(*) AS users FROM cart
UNION 
SELECT 'order_placed' AS action_type,COUNT(*) AS users FROM orders),
x AS (
SELECT action_type,users,LAG(users,1) OVER(ORDER BY (SELECT NULL)) AS prev_step_users
FROM master_data)
SELECT action_type,users,prev_step_users,ROUND(((prev_step_users-users)/prev_step_users)*100,2) AS drop_percentage
FROM x;

-- 2. Genderwise Sales Funnel
WITH visits AS (
SELECT DISTINCT(a.user_id),gender
FROM fact_action a 
JOIN dim_users u 
ON a.user_id=u.id
WHERE action_type='website_visit'),
clicks AS (
SELECT DISTINCT(v.user_id),gender
FROM visits v 
JOIN fact_action a 
ON v.user_id=a.user_id
WHERE action_type='click'),
cart AS (
SELECT DISTINCT(c.user_id),gender
FROM clicks c 
JOIN fact_action a 
ON c.user_id=a.user_id
WHERE action_type='add_to_cart'),
orders AS (
SELECT DISTINCT(c.user_id),gender
FROM cart c 
JOIN fact_action a 
ON c.user_id=a.user_id
WHERE action_type='order'),
master_data AS (
SELECT gender,'website_visit' AS action_type,COUNT(*) AS users FROM visits GROUP BY gender,action_type
UNION
SELECT gender,'click' AS action_type,COUNT(*) AS users FROM clicks GROUP BY gender,action_type
UNION
SELECT gender,'order_cart' AS action_type,COUNT(*) AS users FROM cart GROUP BY gender,action_type
UNION 
SELECT gender,'order_placed' AS action_type,COUNT(*) AS users FROM orders GROUP BY gender,action_type),
x AS (
SELECT gender,action_type,users,LAG(users,1) OVER(PARTITION BY gender ORDER BY (SELECT NULL)) AS prev_step_users
FROM master_data)
SELECT gender,action_type,users,prev_step_users,ROUND(((prev_step_users-users)/prev_step_users)*100,2) AS drop_percentage
FROM x;

-- 3. Age Group wise funnel
WITH users AS (
SELECT id,
CASE WHEN TIMESTAMPDIFF(YEAR,date_of_birth,NOW()) BETWEEN 18 AND 30 THEN '18-30'
WHEN TIMESTAMPDIFF(YEAR,date_of_birth,NOW()) BETWEEN 31 AND 40 THEN '31-40'
WHEN TIMESTAMPDIFF(YEAR,date_of_birth,NOW()) BETWEEN 41 AND 50 THEN '41-50'
WHEN TIMESTAMPDIFF(YEAR,date_of_birth,NOW()) BETWEEN 51 AND 60 THEN '51-60'
WHEN TIMESTAMPDIFF(YEAR,date_of_birth,NOW())>60 THEN '60+' END AS age_group
FROM dim_users),
visits AS (
SELECT DISTINCT(a.user_id),age_group
FROM fact_action a 
JOIN users u 
ON a.user_id=u.id
WHERE action_type='website_visit'),
clicks AS (
SELECT DISTINCT(v.user_id),age_group
FROM visits v 
JOIN fact_action a 
ON v.user_id=a.user_id
WHERE action_type='click'),
cart AS (
SELECT DISTINCT(c.user_id),age_group
FROM clicks c 
JOIN fact_action a 
ON c.user_id=a.user_id
WHERE action_type='add_to_cart'),
orders AS (
SELECT DISTINCT(c.user_id),age_group
FROM cart c 
JOIN fact_action a 
ON c.user_id=a.user_id
WHERE action_type='order'),
master_data AS (
SELECT age_group,'website_visit' AS action_type,COUNT(*) AS users FROM visits GROUP BY age_group,action_type
UNION
SELECT age_group,'click' AS action_type,COUNT(*) AS users FROM clicks GROUP BY age_group,action_type
UNION
SELECT age_group,'order_cart' AS action_type,COUNT(*) AS users FROM cart GROUP BY age_group,action_type
UNION 
SELECT age_group,'order_placed' AS action_type,COUNT(*) AS users FROM orders GROUP BY age_group,action_type),
x AS (
SELECT age_group,action_type,users,LAG(users,1) OVER(PARTITION BY age_group ORDER BY (SELECT NULL)) AS prev_step_users
FROM master_data)
SELECT age_group,action_type,users,prev_step_users,ROUND(((prev_step_users-users)/prev_step_users)*100,2) AS drop_percentage
FROM x;

-- 4. Campaign and non Campaign wise funnel
WITH visits AS (
SELECT DISTINCT(a.user_id),
CASE WHEN campaign_id IS NOT NULL THEN 'Campaign' ELSE 'Non_Campaign' END AS campaign_type
FROM fact_action a 
JOIN dim_users u 
ON a.user_id=u.id
WHERE action_type='website_visit'),
clicks AS (
SELECT DISTINCT(v.user_id),campaign_type
FROM visits v 
JOIN fact_action a 
ON v.user_id=a.user_id
WHERE action_type='click'),
cart AS (
SELECT DISTINCT(c.user_id),campaign_type
FROM clicks c 
JOIN fact_action a 
ON c.user_id=a.user_id
WHERE action_type='add_to_cart'),
orders AS (
SELECT DISTINCT(c.user_id),campaign_type
FROM cart c 
JOIN fact_action a 
ON c.user_id=a.user_id
WHERE action_type='order'),
master_data AS (
SELECT campaign_type,'website_visit' AS action_type,COUNT(*) AS users FROM visits GROUP BY campaign_type,action_type
UNION
SELECT campaign_type,'click' AS action_type,COUNT(*) AS users FROM clicks GROUP BY campaign_type,action_type
UNION
SELECT campaign_type,'order_cart' AS action_type,COUNT(*) AS users FROM cart GROUP BY campaign_type,action_type
UNION 
SELECT campaign_type,'order_placed' AS action_type,COUNT(*) AS users FROM orders GROUP BY campaign_type,action_type),
x AS (
SELECT campaign_type,action_type,users,LAG(users,1) OVER(PARTITION BY campaign_type ORDER BY (SELECT NULL)) AS prev_step_users
FROM master_data)
SELECT campaign_type,action_type,users,prev_step_users,ROUND(((prev_step_users-users)/prev_step_users)*100,2) AS drop_percentage
FROM x;

-- 5. Product Category wise funnel
WITH visits AS (
SELECT DISTINCT(a.user_id),category
FROM fact_action a 
JOIN dim_products p
ON a.product_id=p.id
WHERE action_type='website_visit'),
clicks AS (
SELECT DISTINCT(v.user_id),category
FROM visits v 
JOIN fact_action a 
ON v.user_id=a.user_id
WHERE action_type='click'),
cart AS (
SELECT DISTINCT(c.user_id),category
FROM clicks c 
JOIN fact_action a 
ON c.user_id=a.user_id
WHERE action_type='add_to_cart'),
orders AS (
SELECT DISTINCT(c.user_id),category
FROM cart c 
JOIN fact_action a 
ON c.user_id=a.user_id
WHERE action_type='order'),
master_data AS (
SELECT category,'website_visit' AS action_type,COUNT(*) AS users FROM visits GROUP BY category,action_type
UNION
SELECT category,'click' AS action_type,COUNT(*) AS users FROM clicks GROUP BY category,action_type
UNION
SELECT category,'order_cart' AS action_type,COUNT(*) AS users FROM cart GROUP BY category,action_type
UNION 
SELECT category,'order_placed' AS action_type,COUNT(*) AS users FROM orders GROUP BY category,action_type),
x AS (
SELECT category,action_type,users,LAG(users,1) OVER(PARTITION BY category ORDER BY (SELECT NULL)) AS prev_step_users
FROM master_data)
SELECT category,action_type,users,prev_step_users,ROUND(((prev_step_users-users)/prev_step_users)*100,2) AS drop_percentage
FROM x;

-- 6. Campaign and Non Campaign Sales comparison
WITH campaign_sales AS (
SELECT CASE WHEN campaign_id IS NOT NULL THEN 'Campaign' ELSE 'Non Campaign' END AS campaign_type,
ROUND(SUM(CASE WHEN offer_name='50%_off' THEN (quantity*price)*0.5
WHEN offer_name='Buy_one_get_one_free' THEN (quantity*price)*0.5
WHEN offer_name='500_off' THEN (quantity*price)-500
WHEN offer_name='25%_discount' THEN (quantity*price)*0.75
WHEN offer_name='Buy_one_get_one_free' THEN (quantity*price)*0.5
WHEN offer_name='Buy_two_get_one_free' THEN (quantity*price)*0.67
ELSE (quantity*price) END),2) AS total_sales
FROM fact_orders o 
JOIN dim_products p 
ON o.product_id=p.id
RIGHT JOIN dim_offers d_o
ON d_o.id=o.offer_id
WHERE status='placed'
GROUP BY campaign_type)
SELECT campaign_type,total_sales,ROUND(total_sales*100/SUM(total_sales) OVER(),2) AS percentage_sales
FROM campaign_sales;

-- 7. Sales of Product Categories during campaign & Non campaign
SELECT category,ROUND(SUM(CASE WHEN campaign_id IS NOT NULL THEN 
CASE WHEN offer_name = '50%_off' THEN (quantity * price) * 0.5
WHEN offer_name = 'Buy_one_get_one_free' THEN (quantity * price) * 0.5
WHEN offer_name = '500_off' THEN (quantity * price) - 500
WHEN offer_name = '25%_discount' THEN (quantity * price) * 0.75
WHEN offer_name = 'Buy_two_get_one_free' THEN (quantity * price) * (2/3.0)
ELSE (quantity * price) END ELSE 0 END),2) AS campaign_sales,
ROUND(SUM(CASE WHEN campaign_id IS NULL THEN 
CASE WHEN offer_name = '50%_off' THEN (quantity * price) * 0.5
WHEN offer_name = 'Buy_one_get_one_free' THEN (quantity * price) * 0.5
WHEN offer_name = '500_off' THEN (quantity * price) - 500
WHEN offer_name = '25%_discount' THEN (quantity * price) * 0.75
WHEN offer_name = 'Buy_two_get_one_free' THEN (quantity * price) * (2/3.0)
ELSE (quantity * price) END ELSE 0 END),2) AS non_campaign_sales
FROM fact_orders o 
JOIN dim_products p ON o.product_id = p.id
LEFT JOIN dim_offers d_o ON d_o.id = o.offer_id
WHERE status = 'placed'
GROUP BY category;

-- 6. Age Groupwise Sales
WITH age_group_sales AS (
SELECT CASE WHEN TIMESTAMPDIFF(YEAR,date_of_birth,NOW()) BETWEEN 18 AND 30 THEN '18-30'
WHEN TIMESTAMPDIFF(YEAR,date_of_birth,NOW()) BETWEEN 31 AND 40 THEN '31-40'
WHEN TIMESTAMPDIFF(YEAR,date_of_birth,NOW()) BETWEEN 41 AND 50 THEN '41-50'
WHEN TIMESTAMPDIFF(YEAR,date_of_birth,NOW()) BETWEEN 51 AND 60 THEN '51-60'
ELSE '60+' END AS age_group,
ROUND(SUM(CASE WHEN offer_name='50%_off' THEN (quantity*price)*0.5
WHEN offer_name='Buy_one_get_one_free' THEN (quantity*price)*0.5
WHEN offer_name='500_off' THEN (quantity*price)-500
WHEN offer_name='25%_discount' THEN (quantity*price)*0.75
WHEN offer_name='Buy_one_get_one_free' THEN (quantity*price)*0.5
WHEN offer_name='Buy_two_get_one_free' THEN (quantity*price)*0.67
ELSE (quantity*price) END),2) AS total_sales
FROM dim_users u 
LEFT JOIN fact_orders o 
ON u.id=o.user_id
JOIN dim_products p 
ON o.product_id=p.id
RIGHT JOIN dim_offers d_o
ON d_o.id=o.offer_id
WHERE status='placed'
GROUP BY age_group)
SELECT age_group,total_sales,ROUND(total_sales*100/SUM(total_sales) OVER(),2) AS sales_contribution_percentage
FROM age_group_sales;

-- 7. Order Frenquency wise user count
WITH x AS (
SELECT Order_id,user_id,date
FROM fact_orders
WHERE status='placed'),
y AS (
SELECT order_id,user_id,date,LAG(date,1) OVER(PARTITION BY user_id ORDER BY date) AS prev_date
FROM x),
z AS (
SELECT order_id,user_id,date,prev_date,TIMESTAMPDIFF(DAY,prev_date,date) AS date_diff
FROM y)
SELECT CASE WHEN date_diff<8 THEN '0-7 days'
WHEN date_diff BETWEEN 8 AND 30 THEN '8-30 days'
WHEN date_diff BETWEEN 31 AND 60 THEN '31-60 days'
WHEN date_diff BETWEEN 61 AND 90 THEN '61-90 days'
WHEN date_diff BETWEEN 91 AND 120 THEN '91-120 days'
WHEN date_diff>120 THEN 'more than 120 days' END AS datediff_between_orders,COUNT(DISTINCT(user_id)) AS user_count
FROM z
WHERE date_diff IS NOT NULL
GROUP BY datediff_between_orders
ORDER BY datediff_between_orders;


-- 8. Sales of Product Categories during campaign & Non campaign
SELECT category,ROUND(SUM(CASE WHEN campaign_id IS NOT NULL THEN 
CASE WHEN offer_name = '50%_off' THEN (quantity * price) * 0.5
WHEN offer_name = 'Buy_one_get_one_free' THEN (quantity * price) * 0.5
WHEN offer_name = '500_off' THEN (quantity * price) - 500
WHEN offer_name = '25%_discount' THEN (quantity * price) * 0.75
WHEN offer_name = 'Buy_two_get_one_free' THEN (quantity * price) * (2/3.0)
ELSE (quantity * price) END ELSE 0 END),2) AS campaign_sales,
ROUND(SUM(CASE WHEN campaign_id IS NULL THEN 
CASE WHEN offer_name = '50%_off' THEN (quantity * price) * 0.5
WHEN offer_name = 'Buy_one_get_one_free' THEN (quantity * price) * 0.5
WHEN offer_name = '500_off' THEN (quantity * price) - 500
WHEN offer_name = '25%_discount' THEN (quantity * price) * 0.75
WHEN offer_name = 'Buy_two_get_one_free' THEN (quantity * price) * (2/3.0)
ELSE (quantity * price) END ELSE 0 END),2) AS non_campaign_sales
FROM fact_orders o 
JOIN dim_products p ON o.product_id = p.id
LEFT JOIN dim_offers d_o ON d_o.id = o.offer_id
WHERE status = 'placed'
GROUP BY category;

-- 9. Product category wise monthly sales
SELECT DATE_FORMAT(d.date,'%Y-%m') AS month_name,category,
ROUND(SUM(CASE WHEN offer_name='50%_off' THEN (quantity*price)*0.5
WHEN offer_name='Buy_one_get_one_free' THEN (quantity*price)*0.5
WHEN offer_name='500_off' THEN (quantity*price)-500
WHEN offer_name='25%_discount' THEN (quantity*price)*0.75
WHEN offer_name='Buy_one_get_one_free' THEN (quantity*price)*0.5
WHEN offer_name='Buy_two_get_one_free' THEN (quantity*price)*0.67
ELSE (quantity*price) END),2) AS total_sales
FROM dim_date d 
JOIN fact_orders o 
ON d.date=o.date
JOIN dim_products p 
ON p.id=o.product_id
RIGHT JOIN dim_offers d_o 
ON d_o.id=o.offer_id
WHERE status='placed'
GROUP BY month_name,category;

-- 10. Product Category wise total sales
WITH category_sales AS (
SELECT category,
ROUND(SUM(CASE WHEN offer_name='50%_off' THEN (quantity*price)*0.5
WHEN offer_name='Buy_one_get_one_free' THEN (quantity*price)*0.5
WHEN offer_name='500_off' THEN (quantity*price)-500
WHEN offer_name='25%_discount' THEN (quantity*price)*0.75
WHEN offer_name='Buy_one_get_one_free' THEN (quantity*price)*0.5
WHEN offer_name='Buy_two_get_one_free' THEN (quantity*price)*0.67
ELSE (quantity*price) END),2) AS total_sales
FROM dim_products p 
JOIN fact_orders o 
ON p.id=o.product_id
RIGHT JOIN dim_offers d_o
ON d_o.id=o.offer_id
WHERE status='placed'
GROUP BY category)
SELECT category,total_sales,ROUND(total_sales*100/SUM(total_sales) OVER(),2) AS percentage_contribution
FROM category_sales;

-- 11. Each category top 2 products by sales
WITH top_product AS (
SELECT category,name,ROUND(SUM(CASE WHEN offer_name='50%_off' THEN (quantity*price)*0.5
WHEN offer_name='Buy_one_get_one_free' THEN (quantity*price)*0.5
WHEN offer_name='500_off' THEN (quantity*price)-500
WHEN offer_name='25%_discount' THEN (quantity*price)*0.75
WHEN offer_name='Buy_one_get_one_free' THEN (quantity*price)*0.5
WHEN offer_name='Buy_two_get_one_free' THEN (quantity*price)*0.67
ELSE (quantity*price) END),2) AS total_sales
FROM dim_products p 
JOIN fact_orders o 
ON p.id=o.product_id
RIGHT JOIN dim_offers d_o
ON d_o.id=o.offer_id
WHERE status='placed'
GROUP BY category,name),
product_rank AS (
SELECT category,name AS product_name,total_sales,RANK() OVER(PARTITION BY category ORDER BY total_sales DESC) AS rnk
FROM top_product)
SELECT category,product_name,total_sales
FROM product_rank
WHERE rnk<=2;

-- 12. Each Category Most and Least sales Product
WITH category_sales AS (
SELECT category,name,
ROUND(SUM(CASE WHEN offer_name='50%_off' THEN (quantity*price)*0.5
WHEN offer_name='Buy_one_get_one_free' THEN (quantity*price)*0.5
WHEN offer_name='500_off' THEN (quantity*price)-500
WHEN offer_name='25%_discount' THEN (quantity*price)*0.75
WHEN offer_name='Buy_one_get_one_free' THEN (quantity*price)*0.5
WHEN offer_name='Buy_two_get_one_free' THEN (quantity*price)*0.67
ELSE (quantity*price) END),2) AS total_sales
FROM dim_products p 
JOIN fact_orders o 
ON p.id=o.product_id
RIGHT JOIN dim_offers d_o
ON d_o.id=o.offer_id
WHERE status='placed'
GROUP BY category,name),
top_products AS (
SELECT category,name AS product_name,ROW_NUMBER() OVER(PARTITION BY category ORDER BY total_sales DESC) AS rnk1,
ROW_NUMBER() OVER(PARTITION BY category ORDER BY total_sales ASC) AS rnk2
FROM category_sales)
SELECT category,
GROUP_CONCAT(CASE WHEN rnk1=1 THEN product_name ELSE NULL END) AS most_sales_product,
GROUP_CONCAT(CASE WHEN rnk2=1 THEN product_name ELSE NULL END) AS least_sales_product
FROM top_products
GROUP BY category;


-- 13. Monthly Sales and Month over month change percentage
WITH monthly_sales AS (
SELECT MONTH(d.date) AS m_no,DATE_FORMAT(d.date,'%Y-%m') AS month_name,
ROUND(SUM(CASE WHEN offer_name='50%_off' THEN (quantity*price)*0.5
WHEN offer_name='Buy_one_get_one_free' THEN (quantity*price)*0.5
WHEN offer_name='500_off' THEN (quantity*price)-500
WHEN offer_name='25%_discount' THEN (quantity*price)*0.75
WHEN offer_name='Buy_one_get_one_free' THEN (quantity*price)*0.5
WHEN offer_name='Buy_two_get_one_free' THEN (quantity*price)*0.67
ELSE (quantity*price) END),2) AS total_sales
FROM dim_date d 
JOIN fact_orders o 
ON d.date=o.date
JOIN dim_products p 
ON p.id=o.product_id
RIGHT JOIN dim_offers d_o
ON d_o.id=o.offer_id
WHERE status='placed'
GROUP BY m_no,month_name),
x AS (
SELECT month_name,total_sales AS this_month_sales,LAG(total_sales,1) OVER(ORDER BY month_name) AS prev_month_sales
FROM monthly_sales)
SELECT month_name,this_month_sales,prev_month_sales,ROUND(((this_month_sales/prev_month_sales)-1)*100,2) AS mom_change_percentage
FROM x;

-- 14. Monthly Placed and Cancellation Percentage
SELECT DATE_FORMAT(d.Date,'%Y-%m') AS month,
ROUND(SUM(CASE WHEN status='placed' THEN 1 ELSE 0 END)*100/COUNT(*),2) AS completed_orders,
ROUND(SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END)*100/COUNT(*),2) AS cancelled_orders
FROM dim_date d 
LEFT JOIN fact_orders o 
ON d.date=o.date
GROUP BY month
ORDER BY month;

-- 15. Product Category wise Monthly Cancellation Percentage
SELECT DATE_FORMAT(d.Date,'%Y-%m') AS month,category,
ROUND(SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END)*100/COUNT(*),2) AS cancelled_orders
FROM dim_date d 
LEFT JOIN fact_orders o
ON d.date=o.date
JOIN dim_products p 
ON p.id=o.product_id
GROUP BY month,category
ORDER BY month;

-- 16. Month over Month Cancellation Percentage Change
WITH x AS (
SELECT DATE_FORMAT(d.Date,'%Y-%m') AS month,
ROUND(SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END)*100/COUNT(*),2) AS cancellation_percentage
FROM dim_date d 
LEFT JOIN fact_orders o 
ON d.date=o.date
GROUP BY month),
y AS (
SELECT month,cancellation_percentage AS this_month,LAG(cancellation_percentage,1) OVER(ORDER BY month) AS prev_month
FROM x)
SELECT month,this_month,prev_month,ROUND(((this_month/prev_month)-1)*100,2) AS month_over_month_change
FROM y;

-- 17. offerwise sales
WITH offer_sales AS (
SELECT offer_name,
ROUND(SUM(CASE WHEN offer_name='50%_off' THEN (quantity*price)*0.5
WHEN offer_name='Buy_one_get_one_free' THEN (quantity*price)*0.5
WHEN offer_name='500_off' THEN (quantity*price)-500
WHEN offer_name='25%_discount' THEN (quantity*price)*0.75
WHEN offer_name='Buy_one_get_one_free' THEN (quantity*price)*0.5
WHEN offer_name='Buy_two_get_one_free' THEN (quantity*price)*0.67
ELSE (quantity*price) END),2) AS total_sales
FROM fact_orders o 
JOIN dim_products p 
ON o.product_id=p.id
RIGHT JOIN dim_offers d_o
ON d_o.id=o.offer_id
WHERE status='placed'
GROUP BY offer_name)
SELECT offer_name,total_sales,ROUND(total_sales*100/SUM(total_sales) OVER(),2) AS sales_contribution
FROM offer_sales;

-- 18. Customer Retention rate
WITH x AS (
SELECT DISTINCT(user_id) AS user_id,DATE_FORMAT(d.date,'%Y-%m') AS month_name,MONTH(d.date) AS m
FROM dim_date d 
JOIN fact_orders o 
ON d.date=o.date
WHERE status='placed'),
y AS (
SELECT p.month_name,COUNT(DISTINCT(p.user_id)) AS this_month_users,COUNT(DISTINCT(t.user_id)) next_month_users
FROM x p 
LEFT JOIN x t
ON p.user_id=t.user_id AND t.m=p.m+1 
GROUP BY p.month_name)
SELECT month_name,this_month_users,next_month_users,ROUND(next_month_users*100/this_month_users,2) AS retention_rate
FROM y;