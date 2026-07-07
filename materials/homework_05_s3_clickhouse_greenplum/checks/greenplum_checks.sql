SELECT count(*) AS orders_count FROM ods.ecommerce_orders;
SELECT count(*) AS users_count FROM ods.ecommerce_users;
SELECT count(*) AS items_count FROM ods.ecommerce_order_items;

SELECT *
FROM mart.ecommerce_city_revenue
ORDER BY dt, city;

EXPLAIN
SELECT u.city, count(*)
FROM ods.ecommerce_orders o
JOIN ods.ecommerce_users u ON o.user_id = u.user_id
GROUP BY u.city;
