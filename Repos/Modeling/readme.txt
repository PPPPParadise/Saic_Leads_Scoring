1. ��������
- ģ�������ֱ�Ӷ�csv�ļ���csv����Ҫ���ֶκʹӴ���ɸѡ���ֶβο� mm_big_wide_info_col_used.SQL;
- csv�ļ�û����������tab�ָ����Ϊ��mm_big_wide_info.csv��


2. ģ���ļ�
- ģ���ļ�������

|-------- data_preparation_all.py
|
|-------- data_engineering_auto.py
|
|-------- model_predict_auto.py
|
|-------- run_auto.py


- run_auto.py Ϊʵ�����нű�������data_preparation_all��data_engineering_auto��model_predict_auto


3. ������Ϊ���е����ݿ�:
 -----------------------
|  col_name  | data_type|
|   mobile   |  string  |
| pred_score |   float  |
| result_date| datetime |
|------------|----------|

д��marketing_modeling.mm_model_result�����ű�ᰴpt����
