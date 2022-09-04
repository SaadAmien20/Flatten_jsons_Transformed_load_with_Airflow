import pandas as pd

def load_to_csv (dataframe):
    dataframe.index.name = 'Id' 
    df_new = df_new.to_csv('excercise3.csv')
    print( 'done Successfully')
    return 1

