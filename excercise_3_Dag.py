import datetime 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# from flattened_json import flatten_json
# from Transform_Excercise_2 import transform_layer 
# from load_parsed_json import load_to_csv
project_Id = 'Momox_DE_Excercise'


import json 
import pandas as pd
"""
I thought to make conditions in excercise 2 more easir, should flatten data
"""
def flatten_json (file_path):
    with open (file_path) as source_file:
        data = json.load(source_file)
        
        print ('Flatten Stage is Started')

        outer_columns_metadata = ['timeslotId','campaignId','campaignName','channelName','channelId','position','versionId',\
            'breakCode','airingTimeMedia','price','target1']
    # create empty Df for outer and nested column in metrics list
        df_all = pd.DataFrame(columns=['name','platform','application','type','value','timeslotId','campaignId','campaignName','channelName','channelId','position','versionId',\
            'breakCode','airingTimeMedia','price','target1'])
    # determine how many rows in the file: With Counting outer rows 
        length_row_file = 0
        for row in data['result']:
            for key, val in row.items():
                if key == 'channelName':  # it's mandatory column in data 
                    length_row_file +=1   # length_row_file = 3 in this the file
        for i in range(0,len(data['result'])-length_row_file+1):
            # create dymnamic df and append it to union all rows
            values  = pd.json_normalize(data['result'][i],'metrics',outer_columns_metadata) # pass columns Names and Nested List
            df_all = df_all.append(values,ignore_index=True)
            source_file.close()
        df_all.to_csv('/home/saad/transformed/flatten_josn.csv')
        print( 'Flatten Stage is Ended')

import pandas as pd
#from flattened_json import flatten_json
import numpy as np
colums = [
    'realytics_projectId','channelName','airingTimeMedia','breakCode','target1','versionId','price','TV_visitUplift',
'TV_appInstall_momox', 'TV_webTransaction_momox','TV_androidTransaction_momox','TV_iosTransaction_momox','TV_webRevenue_momox',
'TV_androidRevenue_momox','TV_iosRevenue_momox','TV_webTransaction_newC_momox','TV_webRevenue_newC_momox',
'TV_webTransaction_existingC_momox','TV_webRevenue_existingC_momox','TV_androidTransaction_newC_momox',
'TV_androidTransaction_existingC_momox','TV_iosTransaction_newC_momox','TV_iosTransaction_existingC_momox',
'TV_androidRevenue_newC_momox','TV_androidRevenue_existingC_momox','TV_iosRevenue_newC_momox',
'TV_iosRevenue_existingC_momox','TV_bmOutTab_momox','TV_fashionInTab_momox','TV_fashionin_pageViews_momox',
'TV_fashionin_webTransaction_newC_momox','TV_fashionin_webTransaction_existingC_momox',
'TV_fashionin_webRevenue_newC_momox','TV_fashionin_webRevenue_existingC_momox','TV_fashionin_androidTransaction_existingC_momox',
'TV_fashionin_androidTransaction_newC_momox','TV_fashionin_iosTransaction_existingC_momox','TV_fashionin_iosTransaction_newC_momox',
'TV_fashionin_androidRevenue_existingC_momox','TV_fashionin_androidRevenue_newC_momox',
'TV_fashionin_iosRevenue_existingC_momox','TV_fashionin_iosRevenue_newC_momox','campaignId','airingTimeMedia','platform','campaignName'
    ]
def transform_layer(flaten_objs):
    project_id = 'Momox_DE_Exercise'
    flaten_objs = pd.read_csv(flaten_objs)
    # namefile = 'exercise3.json'
    #Prepare Data for Extraction Layer as Stage layer
    df = pd.DataFrame(columns=colums)
    TV_project_parm= 'ubup'

    print ('Transformaed Stage is Started')

    conditions = [
    (flaten_objs['name']=='ry_session') & (flaten_objs['type']=='EventNb'),
    (flaten_objs['name']=='ry_install') & (flaten_objs['type']=='EventNb'),
    (flaten_objs['name']=='momox_transaction') & (flaten_objs['type'] == 'EventNb'),
    (flaten_objs['name']=='sale_de_android') & (flaten_objs['type'] == 'EventNb'),
    (flaten_objs['name']=='sale_de_ios') & (flaten_objs['type'] == 'EventNb'),
    (flaten_objs['name']=='momox_transaction') & (flaten_objs['type'] == 'EventTxAmount'),
    (flaten_objs['name']=='sale_de_android') & (flaten_objs['type'] == 'EventTxAmount'),
    (flaten_objs['name']=='sale_de_ios') & (flaten_objs['type'] == 'EventTxAmount'),
    (flaten_objs['name']==TV_project_parm+'_transaction_new') & (flaten_objs['type']=='EventNb'),
    (flaten_objs['name']==TV_project_parm+'_transaction_new') & (flaten_objs['type']=='EventTxAmount'),
    (flaten_objs['name']==TV_project_parm+'_transaction_renew') & (flaten_objs['type']=='EventNb'),
    (flaten_objs['name']==TV_project_parm+'_transaction_renew') & (flaten_objs['type']=='EventTxAmount'),
    (flaten_objs['name']=='sale_de_android_new') & (flaten_objs['type'] == 'EventNb'),
    (flaten_objs['name']=='sale_de_android_renew') & (flaten_objs['type'] == 'EventNb'),
    (flaten_objs['name']=='sale_de_ios_new') & (flaten_objs['type'] == 'EventNb'),
    (flaten_objs['name']=='sale_de_ios_renew') & (flaten_objs['type'] == 'EventNb'),
    (flaten_objs['name']=='sale_de_android_new') & (flaten_objs['type']=='EventTxAmount'),
    (flaten_objs['name']=='sale_de_android_renew') & (flaten_objs['type']=='EventTxAmount'),
    (flaten_objs['name']=='sale_de_ios_new') & (flaten_objs['type'] == 'EventTxAmount'),
    (flaten_objs['name']=='sale_de_ios_renew') & (flaten_objs['type'] == 'EventTxAmount'),
    (flaten_objs['name']=='kaufen') & (flaten_objs['type'] == 'EventNb'),
    (flaten_objs['name']=='kleidung') & (flaten_objs['type'] == 'EventNb'),
    (flaten_objs['name']=='fashion_pages') & (flaten_objs['type'] == 'EventNb'),
    (flaten_objs['name']=='fashion_transaction_new') & (flaten_objs['type'] == 'EventNb'),
    (flaten_objs['name']=='fashion_transaction_renew') & (flaten_objs['type'] == 'EventNb'),
    (flaten_objs['name']=='fashion_transaction_new') & (flaten_objs['type'] == 'EventTxAmount'),
    (flaten_objs['name']=='fashion_transaction_renew') & (flaten_objs['type'] == 'EventTxAmount'),
    (flaten_objs['name']=='sale_de_android_renew_fashion') & (flaten_objs['type'] == 'EventNb'),
    (flaten_objs['name']=='sale_de_android_new_fashion') & (flaten_objs['type'] == 'EventNb'),
    (flaten_objs['name']=='sale_de_ios_renew_fashion') & (flaten_objs['type'] == 'EventNb'),
    (flaten_objs['name']=='sale_de_ios_new_fashion') & (flaten_objs['type'] == 'EventNb'),   
    (flaten_objs['name']=='sale_de_android_renew_fashion') & (flaten_objs['type'] == 'EventTxAmount'),
    (flaten_objs['name']=='sale_de_android_new_fashion') & (flaten_objs['type'] == 'EventTxAmount'),
    (flaten_objs['name']=='sale_de_ios_renew_fashion') & (flaten_objs['type'] == 'EventTxAmount'),
    (flaten_objs['name']=='sale_de_ios_new_fashion') & (flaten_objs['type'] == 'EventTxAmount'),   
    ]

    choices = [flaten_objs['value'] for i in range(len(conditions))]

    # Assign Column Variables in the dataframe

    df['channelName']=     flaten_objs['channelName']
    df['airingTimeMedia'] =flaten_objs['airingTimeMedia']
    df['breakCode'] = flaten_objs['breakCode']
    df['target1'] = flaten_objs['target1']
    df['versionId'] = flaten_objs['versionId']
    df['price'] = flaten_objs['price']
    df['TV_visitUplift']= np.select(conditions,choices,default='NA')
    df['TV_appInstall_momox'] = np.select(conditions,choices,default='Na')
    df['TV_webTransaction_momox']=np.select(conditions,choices,default='NA')
    df['TV_androidTransaction_momox']=np.select(conditions,choices,default='NA')
    df['TV_iosTransaction_momox']=np.select(conditions,choices,default='NA')
    df['TV_webRevenue_momox']=np.select(conditions,choices,default='NA')
    df['TV_androidRevenue_momox']=np.select(conditions,choices,default='NA')
    df['TV_iosRevenue_momox']=np.select(conditions,choices,default='NA')
    df['TV_webTransaction_newC_momox']=np.select(conditions,choices,default='NA')
    df['TV_webRevenue_newC_momox']=np.select(conditions,choices,default='NA')
    df['TV_webTransaction_existingC_momox']=np.select(conditions,choices,default='NA')
    df['TV_webRevenue_existingC_momox']=np.select(conditions,choices,default='NA')
    df['TV_androidTransaction_newC_momox']=np.select(conditions,choices,default='NA')
    df['TV_androidTransaction_existingC_momox']=np.select(conditions,choices,default='NA')
    df['TV_iosTransaction_newC_momox']=np.select(conditions,choices,default='NA')
    df['TV_iosTransaction_existingC_momox']=np.select(conditions,choices,default='NA')
    df['TV_androidRevenue_newC_momox']=np.select(conditions,choices,default='NA')
    df['TV_androidRevenue_existingC_momox']=np.select(conditions,choices,default='NA')
    df['TV_iosRevenue_newC_momox']=np.select(conditions,choices,default='NA')
    df['TV_iosRevenue_existingC_momox']=np.select(conditions,choices,default='NA')
    df['TV_bmOutTab_momox']=np.select(conditions,choices,default='NA')
    df['TV_fashionInTab_momox']=np.select(conditions,choices,default='NA')
    df['TV_fashionin_pageViews_momox']=np.select(conditions,choices,default='NA')
    df['TV_fashionin_webTransaction_newC_momox']=np.select(conditions,choices,default='NA')
    df['TV_fashionin_webTransaction_existingC_momox']=np.select(conditions,choices,default='NA')
    df['TV_fashionin_webRevenue_newC_momox']=np.select(conditions,choices,default='NA')
    df['TV_fashionin_webRevenue_existingC_momox']=np.select(conditions,choices,default='NA')
    df['TV_fashionin_androidTransaction_existingC_momox']=np.select(conditions,choices,default='NA')
    df['TV_fashionin_androidTransaction_newC_momox']=np.select(conditions,choices,default='NA')
    df['TV_fashionin_iosTransaction_newC_momox']=np.select(conditions,choices,default='NA')
    df['TV_fashionin_androidRevenue_existingC_momox']=np.select(conditions,choices,default='NA')
    df['TV_fashionin_androidRevenue_newC_momox']=np.select(conditions,choices,default='NA')
    df['TV_fashionin_androidTransaction_newC_momox']=np.select(conditions,choices,default='NA')
    df['TV_fashionin_iosRevenue_existingC_momox']=np.select(conditions,choices,default='NA')
    df['TV_fashionin_iosRevenue_newC_momox']=np.select(conditions,choices,default='NA')
    df['realytics_projectId'] = pd.Series([project_id for i in range(len(df.index))])

    #get these columns as they are required at excercise 3
    df['campaignId'] = flaten_objs['campaignId']
    df['platform'] = flaten_objs['platform']
    df['campaignName']=flaten_objs['campaignName']
 
    df.to_csv('/home/saad/transformed/trnasformed_json.csv')
    print('Transformed Stage is Ended')
# load function just to load tranformaed data to check it afetr transformation 

def load_to_csv (dataframe):
    dataframe = pd.read_csv(dataframe)
    dataframe.index.name = 'Id' 
    df_new= dataframe[['campaignId','airingTimeMedia','platform','campaignName']]
    df_new = df_new.to_csv('excercise3.csv')
    print( 'done Successfully')
    #return 1

DEFAULT_DAG_ARGS = {
    'Owner':'Saad Hassan',
    'depends_on_past': True,
    'retry_delay':datetime.timedelta(minutes=5), # retry run dag if there is failure e.g:File not found yet
    'start_date':datetime.datetime(2022,8,26),
    'project_id': project_Id
}
dag  = DAG (
    'Parse_json_to_csv',
    default_args = DEFAULT_DAG_ARGS,
    description='Afetr Flatten Json file, load it to csv',
    schedule_interval = '50 23 * * *'
)
flattened_json = PythonOperator(
    task_id = 'flatten_nested_json_file',
    python_callable = flatten_json , 
    op_kwargs = {'file_path':'/home/saad/exercise3.json'},
    dag = dag
)
transform_Json = PythonOperator(
    task_id = 'transform_json_file',
    python_callable = transform_layer,
    op_kwargs = {'flaten_objs':'/home/saad/transformed/flatten_josn.csv'},
    dag = dag
)
load_transformed_csv = PythonOperator(
    task_id = 'load_json_to_csv',
    python_callable=load_to_csv,
    op_kwargs={'dataframe':'/home/saad/transformed/trnasformed_json.csv'},
    dag=dag
)
clean_working_tree = BashOperator(
    task_id = 'delete_staged_files',
    bash_command='rm -v /home/saad/transformed/*',
    dag= dag
)


flattened_json >> transform_Json >> load_transformed_csv >> clean_working_tree
