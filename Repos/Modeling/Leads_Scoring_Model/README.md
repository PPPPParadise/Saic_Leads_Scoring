### 1. 结构
```python
├──README.md
├──config.yaml
├──SQL
    |──mm_big_wide_info_col_used.sql       # 从大宽表抓取模型需要的column，产生app_model_input_data.txt
    |──mm_model_application_ingestion.sql            # 模型应用小宽表
    |──mm_model_application_table_creation.sql  # 建表语句
    |──mm_model_result_table_creation.sql       # 建表语句
├──Data_Engineering	
    |──data_preparation_all.py
    |──data_engineering_auto.py
    |──data_engineering_others.py
├──Model_Predicting
    |──model_predict_auto.py
    |──model_predict_others.py
├──Model_Execution.py                # 为实际运行脚本,调用其他py文件和模型文件
├──Model_File                        # 预训练好的模型文件，汽车之家子模型后缀为_auto，非汽车之家子模型后缀为_others
    |──pca_auto.pkl
    |──pca_others.pkl
    |──rf_model_auto.pkl
    |──rf_model_others.pkl
├──Middle_Result_Data                # 存储模型需要输入的一些中间变量
```

### 2. 模型数据输入

模型输入会直接读本地txt或csv文件，txt所需要的字段和从大宽表筛选的字段参考 mm_big_wide_info_col_used.SQL，模型input data的column顺序需要和SQL脚本中的顺序保持一致；
文件没有列名，以tab分割，名称为'app_model_input_data.txt'，如需修改输入文件名字，请参考config.yaml


### 3. 模型结果输出为三列的DataFrame，通过Pysaprk写入marketing_modeling.app_model_result
| col_name | data_type|
|------------|----------|
| mobile | string |
| pred_score | float |
| result_date| datetime |

这张表会按pt分区; 在模型结果输出之后，会执行mm_model_application.SQL，这个脚本会产生基于模型产生的应用小宽表
