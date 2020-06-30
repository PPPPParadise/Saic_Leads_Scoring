set tez.queue.name=${queuename};
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE marketing_modeling.app_model_application
PARTITION (pt)
SELECT
    a.mobile,
    CASE WHEN b.mobile IS NOT NULL THEN 1 ELSE 0 END AS outbound,
    CASE WHEN a.pred_score >= ${thres_upper} THEN '高'
    WHEN a.pred_score BETWEEN ${thres_mid} AND ${thres_upper} THEN '中'
    ELSE '低'
    END AS prob_tag,
    a.pt AS pt
FROM 
    marketing_modeling.app_model_result AS a
LEFT JOIN
( 
    SELECT a.mobile as mobile, a.pred_score
    FROM marketing_modeling.app_model_result a
    WHERE to_date(from_unixtime(UNIX_TIMESTAMP(pt,'yyyyMMdd'))) = CURRENT_DATE()
    AND pred_score >= ${thres_upper}
    AND mobile NOT IN
    (
        SELECT mobile
        FROM marketing_modeling.app_model_application
        WHERE to_date(from_unixtime(UNIX_TIMESTAMP(pt,'yyyyMMdd'))) >= DATE_ADD(CURRENT_DATE(),-7)
        AND outbound = 1
    )
    ORDER BY pred_score
    LIMIT ${targeted_vol}
) AS b
ON a.mobile = b.mobile
WHERE to_date(from_unixtime(UNIX_TIMESTAMP(pt,'yyyyMMdd'))) = current_date()
