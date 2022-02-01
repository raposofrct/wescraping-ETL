from sqlalchemy import create_engine
import logging
import os
from jobs import etl

if __name__=='__main__':
    
    # Logs
    
    # Checking if the log folder exists, if not, it creates
    if not os.path.exists('/Users/nando/Comunidade DS/ds_ao_dev/logs'):
        os.makedirs('/Users/nando/Comunidade DS/ds_ao_dev/logs')
    # Log Configuration
    logging.basicConfig(filename='/Users/nando/Comunidade DS/ds_ao_dev/logs/webscraping.txt',level=logging.DEBUG,format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    
    # Data Collect, Cleaning and Insertion to DB
    
    dados = etl().ProductIDs()
    dados = etl().ProductFeatures(dados)
    dados = etl().DataCleaning(dados)
    etl().DataBase(dados,create_engine('sqlite:///hm_db.sqlite',echo=False))