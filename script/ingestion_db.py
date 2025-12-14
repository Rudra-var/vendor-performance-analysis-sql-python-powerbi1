import pandas as pd 
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
     filename="logs/ingestion_data.log",
     level=logging.DEBUG,
     format="%(asctime)s-%(levelname)s-%(message)s",
     filemode="a"
 )
engine= create_engine('sqlite:///inventory.db')

def ingest_db(data,table_name,engine):
    '''This function  will ingest  thw dataframe into database table'''
    data.to_sql(table_name,con=engine,if_exists='replace',index=False)
def load_raw_data():
    ''' this function  will load  the CSVs  as dataframe  and ingest into db'''
    start=time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            data=pd.read_csv('data/'+file)
            logging.info(f'ingesting{file}in db')
            ingest_db(data,file[:-4],engine)
    end=time.time()
    total_time=(end-start)/60
    logging.info('-----------------ingestion complete------------------')
    logging.info(f'\nTotal Time taken: {total_time} mintue')
if __name__=='__main__':
   load_raw_data()