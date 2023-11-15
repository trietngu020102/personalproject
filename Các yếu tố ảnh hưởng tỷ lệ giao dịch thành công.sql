-- Các yếu tố bên trong
-- Tỷ lệ thành công theo Scenario IDs theo tháng
SELECT  dim_scenario.scenario_id
        , MONTH(transaction_time) [month]
        , COUNT(distinct transaction_id) num_trans
        , COUNT(distinct CASE WHEN status_id = 1 THEN transaction_id END) num_success_trans
        , (SELECT COUNT(transaction_id) FROM fact_transaction_2019) total_trans
        , FORMAT(COUNT(distinct CASE WHEN status_id = 1 THEN transaction_id END)*1.00/COUNT(distinct transaction_id) , 'p') success_rate
        , FORMAT( COUNT(distinct transaction_id)*1.00/(SELECT COUNT(transaction_id) FROM fact_transaction_2019), 'p') volumn_ratio
FROM dim_scenario
RIGHT JOIN fact_transaction_2019
ON dim_scenario.scenario_id = fact_transaction_2019.scenario_id
GROUP BY dim_scenario.scenario_id, MONTH(transaction_time)
ORDER BY scenario_id, [month]

-- Tỷ lệ thành công và tỷ trọng theo Scenario IDs
SELECT  dim_scenario.scenario_id
        , COUNT(distinct transaction_id) num_trans
        , COUNT(distinct CASE WHEN status_id = 1 THEN transaction_id END) num_success_trans
        , (SELECT COUNT(transaction_id) FROM fact_transaction_2019) total_trans
        , FORMAT(COUNT(distinct CASE WHEN status_id = 1 THEN transaction_id END)*1.00/COUNT(distinct transaction_id) , 'p') success_rate
        , FORMAT( COUNT(distinct transaction_id)*1.00/(SELECT COUNT(transaction_id) FROM fact_transaction_2019), 'p') volumn_ratio
FROM dim_scenario
RIGHT JOIN fact_transaction_2019
ON dim_scenario.scenario_id = fact_transaction_2019.scenario_id
GROUP BY dim_scenario.scenario_id
ORDER BY scenario_id

-- Tỷ lệ thành công và tỷ trọng theo nền tảng giao dịch
WITH t1 AS (
SELECT  dim_platform.payment_platform
        , COUNT(distinct transaction_id) num_trans
        , COUNT(distinct CASE WHEN status_id = 1 THEN transaction_id END) num_success_trans
        , (SELECT COUNT(transaction_id) FROM fact_transaction_2019) total_trans
FROM dim_platform
RIGHT JOIN fact_transaction_2019
ON dim_platform.platform_id = fact_transaction_2019.platform_id
GROUP BY dim_platform.payment_platform)
SELECT payment_platform
        ,FORMAT(num_success_trans*1.00/num_trans, 'p') success_rate
        , FORMAT(num_trans*1.00/total_trans, 'p') volumn_ratio
FROM t1

-- Tỷ lệ thành công và tỷ trọng của nền tảng giao dịch theo thời gian
WITH t1 AS (
SELECT  dim_platform.payment_platform
        , MONTH(transaction_time) [month]
        , COUNT(distinct transaction_id) num_trans
        , COUNT(distinct CASE WHEN status_id = 1 THEN transaction_id END) num_success_trans
        , (SELECT COUNT(transaction_id) FROM fact_transaction_2019) total_trans
FROM dim_platform
RIGHT JOIN fact_transaction_2019
ON dim_platform.platform_id = fact_transaction_2019.platform_id
GROUP BY dim_platform.payment_platform, MONTH(transaction_time))
SELECT payment_platform
        , [month]
        ,FORMAT(num_success_trans*1.00/num_trans, 'p') success_rate
        , FORMAT(num_trans*1.00/total_trans, 'p') volumn_ratio
FROM t1
ORDER BY payment_platform, [month]

-- Tỷ lệ thành công và tỷ trọng theo phương thức giao dịch
WITH t1 AS (
SELECT  dim_payment_channel.payment_method
        , COUNT(distinct transaction_id) num_trans
        , COUNT(distinct CASE WHEN status_id = 1 THEN transaction_id END) num_success_trans
        , (SELECT COUNT(transaction_id) FROM fact_transaction_2019) total_trans
FROM dim_payment_channel
RIGHT JOIN fact_transaction_2019
ON dim_payment_channel.payment_channel_id = fact_transaction_2019.payment_channel_id
GROUP BY dim_payment_channel.payment_method
)
SELECT payment_method
        ,FORMAT(num_success_trans*1.00/num_trans, 'p') success_rate
        , FORMAT(num_trans*1.00/total_trans, 'p') volumn_ratio
FROM t1

WITH t1 AS (
SELECT  dim_payment_channel.payment_method
        , MONTH(transaction_time) [month]
        , COUNT(distinct transaction_id) num_trans
        , COUNT(distinct CASE WHEN status_id = 1 THEN transaction_id END) num_success_trans
        , (SELECT COUNT(transaction_id) FROM fact_transaction_2019) total_trans
FROM dim_payment_channel
RIGHT JOIN fact_transaction_2019
ON dim_payment_channel.payment_channel_id = fact_transaction_2019.payment_channel_id
GROUP BY dim_payment_channel.payment_method, MONTH(transaction_time)
)
SELECT payment_method
        , [month]
        ,FORMAT(num_success_trans*1.00/num_trans, 'p') success_rate
        , FORMAT(num_trans*1.00/total_trans, 'p') volumn_ratio
FROM t1
ORDER BY payment_method, [month]

-- Tỷ lệ lỗi theo thời gian
WITH t1 AS (
SELECT  dim_status.status_id
        , MONTH(transaction_time) [month]
        , COUNT(distinct transaction_id) num_trans
        , (SELECT COUNT(transaction_id) FROM fact_transaction_2019) total_trans
FROM dim_status
RIGHT JOIN fact_transaction_2019
ON dim_status.status_id = fact_transaction_2019.status_id
GROUP BY dim_status.status_id, MONTH(transaction_time)
),
t2 AS 
(SELECT status_id
        , [month]
        , num_trans 
FROM t1
),
t3 AS
(SELECT status_id, [1],[2],[3],[4], [5],[6],[7],[8],[9],[10],[11],[12] 
FROM t2 
PIVOT
(
SUM(num_trans)
FOR [month]
IN ([1],[2],[3],[4], [5],[6],[7],[8],[9],[10],[11],[12])
) AS bangnguon)
SELECT status_id
        ,[1] Jan
        , [2] Feb 
        , [3] Mar 
        , [4]  Apr 
        , [5] May 
        , [6] Jun
        , [7] Jul
        , [8] Aug
        , [9] Sep 
        , [10] Oct 
        , [11] Nov 
        , [12] [Dec]
FROM t3

-- Tỷ trọng các loại lỗi giao dịch
WITH t1 AS (
SELECT  dim_status.status_id
        , COUNT(distinct transaction_id) num_trans
        , (SELECT COUNT(transaction_id) FROM fact_transaction_2019) total_trans
FROM dim_status
RIGHT JOIN fact_transaction_2019
ON dim_status.status_id = fact_transaction_2019.status_id
GROUP BY dim_status.status_id
)
SELECT status_id
        , FORMAT(num_trans*1.00/total_trans,'p') AS volumn_ratio
FROM t1


-- Các yếu tố khách quan
-- Tỷ lệ giao dịch thất bại trước giao dịch Top-up account đầu tiên
WITH a1 AS 
(
SELECT distinct customer_id
        , MIN(transaction_time) OVER (PARTITION BY customer_id ORDER BY transaction_time) first_top_up_success_trans
FROM fact_transaction_2019
LEFT JOIN dim_scenario
ON fact_transaction_2019.scenario_id = dim_scenario.scenario_id
WHERE transaction_type LIKE '%Top-up%' 
AND status_id = '1'
),
a2 AS 
(
SELECT fact_transaction_2019.*, a1.first_top_up_success_trans
FROM a1 
JOIN fact_transaction_2019
ON a1.customer_id = fact_transaction_2019.customer_id
)
SELECT customer_id
        , COUNT(distinct transaction_id) num_trans_before 
        , COUNT(CASE WHEN status_id <> '1' THEN transaction_id END) num_failed_trans_before
        , FORMAT(COUNT(CASE WHEN status_id <> '1' THEN transaction_id END)*1.00/COUNT(distinct transaction_id), 'p') pct_fail_before
FROM a2
WHERE transaction_time < first_top_up_success_trans
GROUP BY customer_id

-- Tỷ lệ lỗi do khách hàng khi thực hiện giao dịch
SELECT FORMAT(COUNT(CASE WHEN status_id IN ('-8','-7', '-6','-5','-4','-3','-2') THEN transaction_id END)*1.00/ COUNT(Case when status_id <> '1' THEN transaction_id END),'p') pct_customer_error
FROM fact_transaction_2019

-- Tác động của chương trình khuyến mãi đến tỷ lệ thành công của giao dịch (so sánh trước và sau khi có khuyến mãi)
WITH b1 AS
(
SELECT distinct customer_id
FROM fact_transaction_2019
WHERE promotion_id <> '0'
),
b2 AS 
(
SELECT distinct b1.customer_id
        , MIN(transaction_time) OVER (PARTITION BY b1.customer_id) first_promo_trans
FROM b1
JOIN fact_transaction_2019
ON b1.customer_id = fact_transaction_2019.customer_id
WHERE promotion_id <> '0'
),
b3 AS 
(
SELECT fact_transaction_2019.*, b2.first_promo_trans
FROM b2 
JOIN fact_transaction_2019
ON b2.customer_id = fact_transaction_2019.customer_id
WHERE transaction_time > first_promo_trans
),
b4 AS 
(
SELECT customer_id
        , COUNT(transaction_id) num_trans_after
        , COUNT(CASE WHEN status_id = '1' THEN transaction_id END) num_success_trans_after
        , FORMAT(COUNT(CASE WHEN status_id = '1' THEN transaction_id END)*1.00/COUNT(transaction_id),'p') success_rate_after
FROM b3
GROUP BY customer_id
),
b5 AS 
(
SELECT fact_transaction_2019.*, b2.first_promo_trans
FROM b2 
JOIN fact_transaction_2019
ON b2.customer_id = fact_transaction_2019.customer_id
WHERE transaction_time < first_promo_trans
),
b6 AS
(
SELECT customer_id
        , COUNT(transaction_id) num_trans_before 
        , COUNT(CASE WHEN status_id = '1' THEN transaction_id END) num_success_trans_before
        , FORMAT(COUNT(CASE WHEN status_id = '1' THEN transaction_id END)*1.00/COUNT(transaction_id), 'p') success_rate_before
FROM b5
GROUP BY customer_id)
SELECT b6.*, b4.*
FROM b4 
JOIN b6 
ON b4.customer_id = b6.customer_id