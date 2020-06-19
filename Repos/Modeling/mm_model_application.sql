INSERT INTO marketing_modeling.mm_model_application
PARTITION (pt)
SELECT
    mobile,
    CASE WHEN b.mobile IS NOT NULL THEN 1 ELSE 0 END AS outbound,
    CASE WHEN pred_score >= 0.75 THEN '高'
    WHEN pred_score BETWEEN 0.5 AND 0.75 THEN '中'
    ELSE '低'
    END AS prob_tag
FROM 
    marketing_modeling.mm_model_result AS a
LEFT JOIN
( 
    SELECT TOP 10000
        mobile
    FROM marketing_modeling.mm_model_result
    WHERE partition_date = CURRENT_DATE()
    AND pred_score >= 0.75
    AND mobile IS NOT IN
    (
        SELECT mobile
        FROM marketing_modeling.mm_model_application
        WHERE partition_date >= DATE_ADD(CURRENT_DATE(),-7)
        AND outbound IS NOT NULL
    )
    ORDER BY pred_score
) AS b
ON a.mobile = b.mobile