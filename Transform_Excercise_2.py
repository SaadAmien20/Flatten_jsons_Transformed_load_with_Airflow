import pandas as pd
from flattened_json import flatten_json
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
    
    print ('Transformaed Stage is Ended')
    return df
# load function just to load tranformaed data to check it afetr transformation 

def load_to_csv(datafram):
    datafram.index.name = 'Id' 
    datafram.to_csv('Excercise2_Transformed_file.csv')
    print ('Data Loaded Successfully')
    
if __name__ == '__main__':
    try:
        namefile = r'D:\interview\momox\Momox_DE_Challenge\exercise3.json'
        flatten_objs = flatten_json(namefile)
        transformed_data = transform_layer(flatten_objs)
        load_to_csv(transformed_data)
    except IOError:
        print ('No file by that Name, Please Check the path of the file')
        