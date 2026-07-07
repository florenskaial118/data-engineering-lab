SELECT count() AS orders_count FROM ods.ecommerce_orders;
SELECT count() AS users_count FROM ods.ecommerce_users;
SELECT count() AS items_count FROM ods.ecommerce_order_items;

SELECT *
FROM mart.ecommerce_city_revenue
ORDER BY dt, city;

SELECT *
FROM mart.ecommerce_product_revenue
ORDER BY total_revenue DESC;
