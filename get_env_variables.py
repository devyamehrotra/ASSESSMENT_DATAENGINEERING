import os

os.environ['envn']='DEV'
os.environ['header']='True'
os.environ['inferSchema']='True'

header = os.environ['header']
inferSchema = os.environ['inferSchema']
envn =os.environ['envn']


appName = "Aidetic Assignmnet"
current = os.getcwd()



src_olap = os.path.join(current, 'source', 'olap')
src_oltp = os.path.join(current, 'source', 'oltp')



output_path_parquet = 'output\parquet_output'

output_path_csv = 'output\csv_output'

