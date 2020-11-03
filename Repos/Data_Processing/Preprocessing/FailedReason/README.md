### 1. 结构
```python
├──README.md
├──support_data       
    |──location.txt       # 存储了主要的城市名
├──zhtools                # 第三方提供的数据清理包，我们使用其中的langconv.py脚本来进行中文繁体转化成中文简体
├──Fail_Reason.py	      # 生成战败原因的python脚本
├──mm_failed_reason_table_creation.sql       # 存储战败原因的结果表建表语句，非分区表
├──run_reasons.sh                        # 执行Fai_Reason.py的袋鼠shell脚本
```

### 2. 模型运行
- 通过pyspark从大宽表拉取参数pt的全量数据
- 模型输出结果为一级战败原因（array<string>）和二级战败原因（array<string>），通过pyspark写入marketing_modeling.app_failed_reason