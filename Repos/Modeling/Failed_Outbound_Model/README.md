### 1. 结构
```python
├──README.md
├──config.yaml
├──SQL
    |──Cust_Dealer_Info_Mapping.sql       # 选取每日打分前15000的线索，匹配经销商，战败原因等信息
    |──Insert_Failed_Model_Result.sql            # 模型结果写入一个中间表,把中间表的数据写入marketing_modeling.outbound_model_result
    |──Outbound_Model_Result_Table_Creation.sql  # 存储模型结果的建表语句
├──Input_Data	
    |──Input_Columns.txt                       # 战败模型所用feature
    |──SIS下发经销商名单.xlsx                  # 金线索经销商名单
    |──city_level.txt                          # 城市分级标准（第一财经2019年发布）
├──Output_Data	                               # 存储Outbound_List_Generation.py输出的结果文件
├──Outbound_List_Generation.py                # 特征工程和模型预测过程运行脚本
├──Outbound_Info_Generation.py                # 匹配经销商等标签信息，执行更换经销商过程，外呼名单sftp文件传输
├──Model_File                        # 预训练好的模型文件，模型文件命名方式为{pt_date}_xgb_model1/{pt_date}_xgb_model2
├──outbound_run.sh                   # 袋鼠执行的shell脚本
```

### 2. 模型运行

- 模型进行预测时会通过pyspark从大宽表抓取参数pt的所有完全战败用户的数据
- Model_File文件夹会存储历史所有训练过程产生的模型文件，模型预测时抓取更新时间为最新的模型文件进行使用
- 模型结果通过pyspark写入临时表marketing_modeling.tmp_failed_model_result，再通过运行Insert_Failed_Model_Result.sql把临时表中结果写入marketing_modeling.outbound_model_result
- marketing_modeling.outbound_model_result为分区表，每个分区会存储该pt下全量完全战败用户的模型打分