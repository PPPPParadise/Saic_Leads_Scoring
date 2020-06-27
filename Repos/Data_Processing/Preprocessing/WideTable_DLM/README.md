### 1. 结构
```SQL
├──README.md
├──load_ods_data                   #加载ods的各种数据
    |──load_dlm_t_leads_m          #留资数据
        |──
    |──load_dlm_t_cust_base
        |──
    |──load_dlm_t_followup
        |──
    |──load_dlm_t_oppor
        |──
    |──load_dlm_t_oppor_fail
        |──
    |──load_dlm_t_deliver_vel
        |──
    |──load_dlm_t_hall_flow
        |──
    |──load_dlm_t_trial_receive
        |──
    |──load_dlm_t_cust_vehicle
        |──
    |──load_dlm_t_cust_activity
        |──
├──processing_feature_step1

├──processing_feature_step2

├──processing_feature_step3

├──processing_feature_step1

    |──mm_big_wide_info_col_used.sql         # 从大宽表抓取模型需要的column
    |──mm_model_application.sql            # 模型应用小宽表
    |──mm_model_application_create.sql
    |──mm_model_result_create.sql
├──model_file                        # 预训练好的模型文件
    |──pca.pkl
    |──pca_others.pkl
    |──rf_model.pkl
    |──rf_model_others.pkl
├──data_preparation_all.py
├──data_engineering_auto.py
├──data_engineering_others.py
├──model_predict_auto.py
├──model_predict_others.py
├──run_auto.py                      # 为实际运行脚本,调用其他py文件和模型文件
```


### 2. 模型数据输入

模型输入会直接读csv文件，csv所需要的字段和从大宽表筛选的字段参考 mm_big_wide_info_col_used.SQL;
csv文件没有列名，以tab分割，名称为“mm_big_wide_info.csv”


### 3. 模型结果输出为三列的DataFrame
| col_name | data_type|
|------------|----------|
| mobile | string |
| pred_score | float |
| result_date| datetime |

写入marketing_modeling.app_model_result，这张表会按pt分区


### 4. 支持文件:
```python
|-------- city_level.txt
|-------- city_lift.txt
```
为支持性文件，为特征工程提供信息