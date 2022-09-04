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
        print ('Flatten Stage is Ended')
        return df_all
        

         
