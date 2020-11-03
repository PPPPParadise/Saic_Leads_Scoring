### 1. 结构
```python
├──README.md
├──config.yaml
├──Training_Data            # 存储Feature_Engineering.py执行结束之后产生的一张临时线下表outbound_features.csv，每次训练过程产生的新临时表会直接覆盖旧表
├──Feature_Engineering.py   # 特征工程运行脚本
├──Model_Running.py         # 预测过程运行脚本，训练生成的模型文件会存入Modeling/Failed_Outbound_Model/Model_File
├──Model_Performance.csv    # 存储每次模型训练的test set表现，包括accuracy/precision/recall/f1四个score
```

### 2. 模型数据输入
- 模型训练时会通过pyspark从marketing_modeling.app_sis_model_features拉取参数pt下的全量数据，marketing_modeling.app_sis_model_features为模型训练过程所使用的大宽表
- 模型训练过程的输出为两个模型文件，存入Modeling/Failed_Outbound_Model/Model_File