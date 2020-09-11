set tez.queue.name=${queuename};
DROP TABLE IF EXISTS marketing_modeling.tmp_funcs_processing;
CREATE TABLE  marketing_modeling.tmp_funcs_processing AS
	SELECT a.mobile,
           func as funcs
	FROM
	(
	SELECT  t.mobile,
			replace(replace(t.h_func_prefer,'[',''),']','') as func_pref
	FROM 
		(SELECT mobile, h_func_prefer 
		 FROM marketing_modeling.app_big_wide_info
		 WHERE   pt='${pt}' and 
				 h_func_prefer is NOT NULL and 
				 h_func_prefer != '[]'
		) t,
		marketing_modeling.tmp_cdp_tb tt
	WHERE 
		t.mobile = tt.id and tt.tagid=912186629160964
	) a
	LATERAL VIEW explode(split(a.func_pref,',')) tt as func 