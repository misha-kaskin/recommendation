SELECT l.product_id, r.product_name, l.counts
FROM (
	SELECT product_id, count(order_id) counts
	FROM order_products__prior
	WHERE order_id IN (
		SELECT order_id
		FROM orders
		WHERE user_id IN (
			SELECT user_id
			FROM kmeans_summary
			WHERE clusters = ?
		)
	)
	GROUP BY product_id
	ORDER BY counts DESC
	LIMIT ?
) l;


SELECT t1.clusters, t1.users, t1.products, t1.orders, t2.order_dow, t2.order_hour_of_day, t2.days_since_prior_order
FROM (
	SELECT t.clusters, count(t.user_id) users, avg(t.products) products, avg(t.orders) orders
	FROM (
		SELECT l1.clusters, l1.user_id, count(r1.product_id) products, count(DISTINCT l1.order_id) orders
		FROM (
			SELECT l.clusters, l.user_id, r.order_id
			FROM kmeans_summary l
			JOIN orders r ON (l.user_id = r.user_id AND r.eval_set LIKE 'prior')
		) l1
		JOIN order_products__prior r1 ON (l1.order_id = r1.order_id)
		GROUP BY l1.clusters, l1.user_id
	) t
	GROUP BY clusters
) t1
JOIN (
	SELECT k.clusters, avg(o.order_dow) order_dow, avg(o.order_hour_of_day) order_hour_of_day, avg(o.days_since_prior_order) days_since_prior_order
	FROM kmeans_summary k
	JOIN orders o ON (k.user_id = o.user_id)
	GROUP BY k.clusters
) t2 ON (t1.clusters = t2.clusters)
ORDER BY users DESC;