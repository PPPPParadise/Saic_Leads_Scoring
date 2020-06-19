1. 数据输入
- 模型输入会直接读csv文件，csv所需要的字段和从大宽表筛选的字段参考 mm_big_wide_info_col_used.SQL;
- csv文件没有列名，以tab分割，名称为“mm_big_wide_info.csv”


2. 模型文件
- 模型文件包括：

|-------- data_preparation_all.py
|
|-------- data_engineering_auto.py
|
|-------- model_predict_auto.py
|
|-------- run_auto.py


- run_auto.py 为实际运行脚本，调用data_preparation_all，data_engineering_auto，model_predict_auto


3. 结果输出为三列的数据框:
 -----------------------
|  col_name  | data_type|
|   mobile   |  string  |
| pred_score |   float  |
| result_date| datetime |
|------------|----------|

写入marketing_modeling.mm_model_result，这张表会按pt分区
