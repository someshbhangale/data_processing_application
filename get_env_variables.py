import os

os.environ['envn'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'


header = os.environ['header']
inferSchema = os.environ['inferSchema']
envn = os.environ['envn']

appName = 'data_processing_application'

current = os.getcwd()
#print(os.getcwd())

src_olap = current + '\Source\olap'
src_oltp = current + '\Source\oltp'

#output_path = 'Output\cities'
city_path = 'Output\cities'
presc_path = 'Output\prescriber'