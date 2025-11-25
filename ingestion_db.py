import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    filename= 'logs/ingestion_db.log',
    level=logging.DEBUG,
    formate='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    ''' This function will ingest the dataframe into data'''
    df.to_sql(table_name, con = engine, if_exists= 'replace', index= False)
def load_raw_data():
    start = time.time()
    for file in os.listdir('Data'):
        if '.csv' in file:
            df = pd.read_csv('data/' + file)
            logging.info('ingesting {file} in db ')
            ingest_db(df, file[:-4], engine)
    end = time.time()        
    total_time= (end - start)/60
    logging.info('-----Ingestion Complete-----')
    logging.info(f'\nTotal Time Taken: {Total_time} minnutes')

if __name__ == "__main__":
    load_row_data()