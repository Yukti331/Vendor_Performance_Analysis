import pandas as pd
import os
from sqlalchemy import create_engine
import psycopg2
import logging
import time

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# PostgreSQL connection (change credentials as per your setup)
engine = create_engine('postgresql://postgres:1234@localhost:5432/mydb')

def ingest_db(df, table_name, engine):
    '''this function will ingest the dataframe into table'''
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

def load_raw_data():
    '''this function will load the CSV's as dataframe and ingest into db'''
    data_folder = 'data'
    start=time.time()

    for file in os.listdir(data_folder):
        if file.endswith('.csv'):
            file_path = os.path.join(data_folder, file)
            logging.info(f'ingesting {file} in db')
            table_name = file[:-4]  # Remove '.csv' from file name
            

            chunksize = 50000  # Adjust chunk size as needed

            # First chunk will 'replace' the table, others will 'append'
            first_chunk = True
        
            for chunk in pd.read_csv(file_path, chunksize=chunksize, low_memory=False):
                if first_chunk:
                    chunk.to_sql(table_name, con=engine, if_exists='replace', index=False)
                    first_chunk = False
                else:
                    ingest_db(chunk, table_name, engine)
        
            print(f'Finished ingesting {file} in chunks.')
            end=time.time()
            total_time=(end-start)/60
            logging.info('--------Ingestion complete-------')
            logging.info(f'\ntotal time taken:{total_time} minutes')
            
if __name__=='__main__':
    load_raw_data()