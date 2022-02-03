# Imports
from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
import numpy as np
import warnings
import datetime
import sqlite3
from sqlalchemy import create_engine
import logging
import os

# Jobs
def ProductIDs():
    logging.info("Get Product ID's")
    # Conseguindo o HTML
    html = requests.get('https://www2.hm.com/en_us/men/products/jeans.html', headers={'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'})
    soup = BeautifulSoup(html.text,'html.parser')

    # Conseguindo todas as vitrines (Paginação)
    qtd_produtos = int(soup.find('h2',class_='load-more-heading')['data-total'])
    html = requests.get('https://www2.hm.com/en_us/men/products/jeans.html?sort=stock&image-size=small&image=model&offset=0&page-size='+str(qtd_produtos), headers={'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'})
    soup = BeautifulSoup(html.text,'html.parser')

    # Achar cada vitrine
    produtos = soup.findAll('article',class_='hm-product-item')

    # Conseguindo o link de cada vitrine
    links = []
    for produto in produtos:
        links.append('https://www2.hm.com/'+produto.find('a')['href'])

    # Achar o código do produto
    lst_codigo = []
    for link in links:
        html = requests.get(link, headers={'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'})
        soup = BeautifulSoup(html.text,'html.parser')
        for c in range(len(soup.findAll('a',class_='filter-option miniature'))):
            lst_codigo.append(soup.findAll('a',class_='filter-option miniature')[c]['data-articlecode'])
            logging.debug(f"Product ID {soup.findAll('a',class_='filter-option miniature')[c]['data-articlecode']}")
        for c in range(len(soup.findAll('a',class_='filter-option miniature active'))):
            lst_codigo.append(soup.findAll('a',class_='filter-option miniature active')[c]['data-articlecode'])
            logging.debug(f"Product ID {soup.findAll('a',class_='filter-option miniature active')[c]['data-articlecode']}")

    # Removendo os códigos duplicates (Granularidade)
    dados = pd.DataFrame(lst_codigo,columns=['id'])
    dados.drop_duplicates(subset=['id'],inplace=True)
    dados.reset_index(inplace=True,drop=True)

    return dados

def ProductFeatures(dados):
    logging.info("Get Product Feature's")
    # Obtendo os Links
    links = []
    for cod in dados['id']:
        links.append('https://www2.hm.com/en_us/productpage.'+ cod +'.html')
    dados['link'] = links

    # Nome
    lst_name = []
    for link in dados['link']:
        html = requests.get(link, headers={'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'})
        soup = BeautifulSoup(html.text,'html.parser')
        lst_name.append(' '.join(soup.find('h1',class_='primary product-item-headline').string.split()))
        logging.debug(f"Name {link}")
    dados['name'] = lst_name

    # Cores
    cores = []
    for link in dados['link']:
        html = requests.get(link, headers={'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'})
        soup = BeautifulSoup(html.text,'html.parser')
        cor = soup.find('a',class_='filter-option miniature active')['data-color']
        cores.append(cor)
        logging.debug(f"Color {link}")
    dados['color'] = cores

    # Características
    descricao = {}
    lst = []
    for link in dados['link']:
        html = requests.get(link, headers={'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'})
        soup = BeautifulSoup(html.text,'html.parser')
        for x in soup.findAll('div',class_='pdp-description-list-item'):
            if x.select('li') == []:
                keys = str(x.select('dt'))
                values = str(x.select('dd'))
                descricao[keys] = values
            else:
                keys = str(x.select('dt'))
                values = str(x.select('li'))
                descricao[keys] = values
        lst.append(descricao.copy())
        descricao.clear()
        logging.debug(f"Characteristics {link}")
    descricao = pd.DataFrame(lst)
    descricao.drop('[<dt>Art. No.</dt>]',1,inplace=True)
    dados = pd.concat([dados,descricao],axis=1)

    # Descrição
    descs = []
    for link in dados['link']:
        html = requests.get(link, headers={'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'})
        soup = BeautifulSoup(html.text,'html.parser')
        desc = soup.find('p',class_='pdp-description-text').get_text()
        descs.append(desc)
        logging.debug(f"Description {link}")
    dados['description'] = descs

    # Price
    lst_price = []
    for link in dados['link']:
        html = requests.get(link, headers={'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'})
        soup = BeautifulSoup(html.text,'html.parser')
        soup = soup.find('section',class_='name-price')
        lst_price.append(soup.find('span').get_text().split()[0])
        logging.debug(f"Price {link}")
    dados['price'] = lst_price


    # Add timestamp
    dados['timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    return dados

def DataCleaning(dados):
    logging.info('Cleaning the data')
    # Rename columns
    columns_name_list = []
    for coluna in dados.columns.values:
        try:
            columns_name_list.append((re.search('>(.+)<',coluna).group(1)).lower())
        except:
            columns_name_list.append(coluna.lower())
    dados.columns = columns_name_list

    # Name - pattern
    dados['name'] = dados['name'].apply(lambda x: x.replace(' ','_').lower())

    # Color - Pattern
    dados['color'] = dados['color'].apply(lambda x: x.replace(' ','_').lower())
    dados['color'] = dados['color'].apply(lambda x: x.replace('-','_').lower())

    # Fit - Pattern
    dados['fit'] = dados['fit'].apply(lambda x: (re.search('>(.+)<',x).group(1)))
    dados['fit'] = dados['fit'].apply(lambda x: x.replace(' ','_').lower())

    # Size - Drop
    dados.drop('size',1,inplace=True)

    # Product Safety - Drop
    dados.drop('product safety',1,inplace=True)

    # Price - Pattern
    dados['price'] = dados['price'].apply(lambda x: float(x.split('$')[-1]))

    # Id Group - Create
    dados['id_group'] = dados['id'].apply(lambda x: int(str(x)[:7]))
    dados['id_group'] = dados['id_group'].apply(lambda x: '0'+str(x)) # add 0 no início, como no product id e transformar em str

    # More Sustainable Materials - Drop
    dados.drop('more sustainable materials',axis=1,inplace=True)

    logging.info('Starting Composition Cleaning')
    #######################################################################################################################################################################################################

    # Composition - Pattern 
    dados['composition'] = dados['composition'].apply(lambda x: x.lower())

    # Composition - Separate into several columns
    dados['composition'] = dados['composition'].apply(lambda x: (re.search('>(.+)<',x).group(1)))
    dados['composition'] = dados['composition'].apply(lambda x: x.replace('</li>',''))
    dados['composition'] = dados['composition'].apply(lambda x: x.replace('<li>',''))
    df_aux = dados.copy()
    df_aux['composition_list'] = np.nan

    # O que fazer com compositions nulas
    index_comp_nan = df_aux[df_aux.isna()['composition']].index.values
    for index in index_comp_nan:
        df_aux.loc[index_comp_nan,'composition_list'] = 'Não Fornecido'

    # o que fazer com Clean Compositions
    for c in range(dados.shape[0]):
        lista_comp = re.findall('(\d{1,3})%',dados.loc[c,'composition'])
        for i in range(len(lista_comp)):
            lista_comp[i] = float(lista_comp[i])
        soma = sum(lista_comp)
        if soma == 100 and ':' not in dados.loc[c,'composition']:
            df_aux['composition_list'][c] = list(filter(None,re.split('%|, | ',dados.loc[c,'composition'].replace(', ',''))))


    # retirando o pocket do composition
    # quem só tiver pocket é dado como não fornecido

    ###############################################################################################################

    # Ver os que restam sem composition no comp_list
    index_comp_nao_resolvida = df_aux[df_aux.isna()['composition_list']].index.values

    # Ver, desses sem comp, quais possuem a palavra pocket nele
    l = []
    ind = []
    for index in index_comp_nao_resolvida:
        try:
            l.append(str(re.search('(pocket.+)',df_aux.loc[index,'composition']).group(1)))
            ind.append(index)
        except:
            pass

    # criar um df dos que tem pocket na comp
    df_pocket = pd.DataFrame(l,ind)
    df_pocket.reset_index(inplace=True)
    df_pocket.rename(columns={0:'comp'},inplace=True)
    df_pocket = df_pocket.join(df_aux.loc[index_comp_nao_resolvida,'composition'],on='index')

    # Extraindo as porcentagens
    df_pocket['pct'] = np.nan
    for c in range(df_pocket.shape[0]):
        df_pocket['pct'][c] = re.findall('(\d{1,3})%',df_pocket.loc[c,'comp'])

    # Separar as porcentagens de 100 em 100, a primeira é a do pocket (pct_pocket), as demais não (pct_clothes)
    pct_pocket = []
    pct_clothes = []
    for numeros in df_pocket['pct']:
        soma_anterior = 0
        soma_final = 0
        count = 0
        for num in numeros:
                num = int(num)
                soma_final = soma_anterior + num
                count = count + 1
                soma_anterior = soma_final
                if soma_final == 100:
                    pct_clothes.append(numeros[count:].copy())
                    pct_pocket.append(numeros[:count].copy())
    df_pocket['pct_clothes'] = pct_clothes
    df_pocket['pct_pocket'] = pct_pocket

    # Tirando só a parte do pocket das compositions
    lst_sem_pocket = []
    for index in range(df_pocket.shape[0]):
        frase = df_pocket.loc[index,'composition']
        pattern = '(pocket.+{}%)'.format(df_pocket.loc[index,'pct_pocket'][-1])
        lst_sem_pocket.append(frase.replace(re.search(pattern,frase).group(1),''))
    df_pocket = pd.concat([df_pocket,pd.Series(lst_sem_pocket)],axis=1)
    df_pocket.rename(columns={0:'composition_sem_pocket'},inplace=True)

    # se sobrar nada? os vazios n tem composition (pq eles só tinham pocket)
    for index in range(df_pocket.shape[0]):
        if df_pocket.loc[index,'composition_sem_pocket'] == ' ':
            df_aux.loc[df_pocket.loc[index,'index'],'composition_list'] = 'Não Fornecido'

    # se sobrar só as composições, quero colocá-las no meu composition list
    for c in range(df_pocket.shape[0]):
        lista_comp = re.findall('(\d{1,3})%',df_pocket.loc[c,'composition_sem_pocket'])
        for i in range(len(lista_comp)):
            lista_comp[i] = float(lista_comp[i])
        soma = sum(lista_comp)
        if soma == 100 and ':' not in df_pocket.loc[c,'composition_sem_pocket']:
            df_aux['composition_list'][df_pocket.loc[c,'index']] = list(filter(None,re.split('%|, | ',df_pocket.loc[c,'composition_sem_pocket'].replace(', ',''))))

    # criar nova coluna com os compositions, só que sem o pocket
    df_pocket.set_index('index',inplace=True)
    df_aux['composition_sem_pocket'] = np.nan
    for c in range(df_aux.shape[0]):
        try:
            df_aux['composition_sem_pocket'][c] = df_pocket['composition_sem_pocket'][c]
        except:
            df_aux['composition_sem_pocket'][c] = df_aux['composition'][c]


    # retirando o lining do composition
    # quem só tiver lining é dado como não fornecido

    ################################################################################################################

    # Ver os que restam sem composition no comp_list
    index_comp_nao_resolvida = df_aux[df_aux.isna()['composition_list']].index.values

    # Ver, desses sem comp e sem pocket, quais possuem a palavra lining nele
    l = []
    ind = []
    for index in index_comp_nao_resolvida:
        try:
            l.append(str(re.search('(lining.+)',df_aux.loc[index,'composition_sem_pocket']).group(1)))
            ind.append(index)
        except:
            pass
    # criar um df dos que tem lining e não tem pocket
    df_lining = pd.DataFrame(l,ind)
    df_lining.reset_index(inplace=True)
    df_lining.rename(columns={0:'comp'},inplace=True)
    df_lining = df_lining.join(df_aux.loc[index_comp_nao_resolvida,'composition_sem_pocket'],on='index')

    # Extraindo as porcentagens
    df_lining['pct'] = np.nan
    for c in range(df_lining.shape[0]):
        df_lining['pct'][c] = re.findall('(\d{1,3})%',df_lining.loc[c,'comp'])

    # Separar as porcentagens de 100 em 100, a primeira é a do lining (pct_lining), as demais não (pct_clothes)
    pct_lining = []
    pct_clothes = []
    for numeros in df_lining['pct']:
        soma_anterior = 0
        soma_final = 0
        count = 0
        for num in numeros:
                num = int(num)
                soma_final = soma_anterior + num
                count = count + 1
                soma_anterior = soma_final
                if soma_final == 100:
                    pct_clothes.append(numeros[count:].copy())
                    pct_lining.append(numeros[:count].copy())
    df_lining['pct_clothes'] = pct_clothes
    df_lining['pct_lining'] = pct_lining

    # Tirando só a parte do lining das comp
    lst_sem_lining = []
    for index in range(df_lining.shape[0]):
            frase = df_lining.loc[index,'composition_sem_pocket']
            pattern = '(lining.+{}%)'.format(df_lining.loc[index,'pct_lining'][-1])
            lst_sem_lining.append(frase.replace(re.search(pattern,frase).group(1),''))
    df_lining = pd.concat([df_lining,pd.Series(lst_sem_lining)],axis=1)
    df_lining.rename(columns={0:'comp_sem_lining'},inplace=True)

    # se sobrar nada? os vazios n tem composition (pq eles só tinham lining ou só lining e pocket)
    for index in range(df_lining.shape[0]):
        if df_lining.loc[index,'comp_sem_lining'] == ' ':
            df_aux.loc[df_lining.loc[index,'index'],'composition_list'] = 'Não fornecido'

    # se sobrar só as composições, quero colocá-las no meu composition list
    for c in range(df_lining.shape[0]):
        lista_comp = re.findall('(\d{1,3})%',df_lining.loc[c,'comp_sem_lining'])
        for i in range(len(lista_comp)):
            lista_comp[i] = float(lista_comp[i])
        soma = sum(lista_comp)
        if soma == 100 and ':' not in df_lining.loc[c,'comp_sem_lining']:
            df_aux['composition_list'][df_lining.loc[c,'index']] = list(filter(None,re.split('%|, | ',df_lining.loc[c,'comp_sem_lining'].replace(', ',''))))

    # criar nova coluna com os compositions, só que sem o pocket e sem o lining
    df_lining.set_index('index',inplace=True)
    df_aux['composition_sem_pocket_e_sem_lining'] = np.nan
    for c in range(df_aux.shape[0]):
        try:
            df_aux['composition_sem_pocket_e_sem_lining'][c] = df_lining['comp_sem_lining'][c]
        except:
            df_aux['composition_sem_pocket_e_sem_lining'][c] = df_aux['composition_sem_pocket'][c] # mantenho o sem pocket, se já n tiver lining


    # retirando o shell do composition

    # quem só tiver shell a gente bota shell
    # quem tiver shell + comp a gente bota comp
    # quem tiver outra categoria a gente bota "inconclusivo"

    ################################################################################################################

    # Ver os que restam sem composition no comp_list
    index_comp_nao_resolvida = df_aux[df_aux.isna()['composition_list']].index.values

    # Ver, desses sem comp e sem pocket, quais possuem a palavra lining nele
    l = []
    ind = []
    for index in index_comp_nao_resolvida:
        try:
            l.append(str(re.search('(shell.+)',df_aux.loc[index,'composition_sem_pocket_e_sem_lining']).group(1)))
            ind.append(index)
        except:
            pass
    # criar um df dos que tem shell e não tem pocket nem lining
    df_shell = pd.DataFrame(l,ind)
    df_shell.reset_index(inplace=True)
    df_shell.rename(columns={0:'comp'},inplace=True)
    df_shell = df_shell.join(df_aux.loc[index_comp_nao_resolvida,'composition_sem_pocket_e_sem_lining'],on='index')

    # Extraindo as porcentagens
    df_shell['pct'] = np.nan
    for c in range(df_shell.shape[0]):
        df_shell['pct'][c] = re.findall('(\d{1,3})%',df_shell.loc[c,'comp'])

    # Separar as porcentagens de 100 em 100, a primeira é a do shell (pct_shell), as demais não (pct_clothes)
    pct_shell = []
    pct_clothes = []
    for numeros in df_shell['pct']:
        soma_anterior = 0
        soma_final = 0
        count = 0
        for num in numeros:
                num = int(num)
                soma_final = soma_anterior + num
                count = count + 1
                soma_anterior = soma_final
                if soma_final == 100:
                    pct_clothes.append(numeros[count:].copy())
                    pct_shell.append(numeros[:count].copy())
    df_shell['pct_clothes'] = pct_clothes
    df_shell['pct_shell'] = pct_shell

    # Tirando só a parte do shell das comp
    lst_sem_shell = []
    for index in range(df_shell.shape[0]):
            frase = df_shell.loc[index,'composition_sem_pocket_e_sem_lining']
            pattern = '(shell.+{}%)'.format(df_shell.loc[index,'pct_shell'][-1])
            lst_sem_shell.append(frase.replace(re.search(pattern,frase).group(1),''))
    df_shell = pd.concat([df_shell,pd.Series(lst_sem_shell)],axis=1)
    df_shell.rename(columns={0:'comp_sem_shell'},inplace=True)

    # se sobrar nada? os vazios so tinham shell, ent vamos usar eles
    for c in range(df_shell.shape[0]):
        if not df_shell.loc[c,'comp_sem_shell'].isalpha():
            df_aux['composition_list'][df_shell.loc[c,'index']] = list(filter(None,re.split('%|, | ',df_shell.loc[c,'composition_sem_pocket_e_sem_lining'].replace(', ','').replace('shell: ',''))))

    # se sobrar só as composições, quero colocá-las no meu composition list
    for c in range(df_shell.shape[0]):
        lista_comp = re.findall('(\d{1,3})%',df_shell.loc[c,'comp_sem_shell'])
        for i in range(len(lista_comp)):
            lista_comp[i] = float(lista_comp[i])
        soma = sum(lista_comp)
        if soma == 100 and ':' not in df_shell.loc[c,'comp_sem_shell']:
            df_aux['composition_list'][df_shell.loc[c,'index']] = list(filter(None,re.split('%|, | ',df_shell.loc[c,'comp_sem_shell'].replace(', ',''))))

    # criar nova coluna com os compositions, só que sem o pocket sem o lining e sem o shell
    df_shell.set_index('index',inplace=True)
    df_aux['composition_sem_pocket_e_sem_lining_e_sem_shell'] = np.nan
    for c in range(df_aux.shape[0]):
        try:
            df_aux['composition_sem_pocket_e_sem_lining_e_sem_shell'][c] = df_lining['comp_sem_shell'][c]
        except:
            df_aux['composition_sem_pocket_e_sem_lining_e_sem_shell'][c] = df_aux['composition_sem_pocket_e_sem_lining'][c] # mantenho o sem pocket e sem lining, se já n tiver shell


    # Ver os que restam sem composition no comp_list
    index_comp_nao_resolvida = df_aux[df_aux.isna()['composition_list']].index.values
    for index in index_comp_nao_resolvida:
        df_aux.loc[index_comp_nao_resolvida,'composition_list'] = 'Inconclusivo'

    dados = pd.concat([dados,df_aux['composition_list']],axis=1)

    # Transformando as composições em colunas
    comp_dict = {}
    comp_list = []

    for comp in dados['composition_list']:
        for c in range(0,len(comp),2):
            comp_dict[comp[c]] = comp[c+1]
        comp_list.append(comp_dict.copy())

    df_compositions = pd.DataFrame(comp_list).fillna(0)

    for columns in df_compositions: # convert to float
        df_compositions[columns] = df_compositions[columns].astype('float')

    dados = pd.concat([dados,df_compositions],axis=1)

    dados.drop(['composition','composition_list'],axis=1,inplace=True) # Drop other composition columns

    try: # Se não existir uma coluna com esse nome, só ignora! (Bom para generalizar)
        dados.rename(columns={'elasterell-p':'elasterell_p'},inplace=True) # Esse ' - ' vai nos dar problema no database
    except:
        pass

    return dados


def DataBase(dados,db):
    logging.info('Send to DataBase')
    # Connecting with the Database
    conn = db.connect()

    # Exportando o dataframe
    dados.to_sql('men_jeans',conn,if_exists='append',index=False)

    logging.info('Everything was fine')

    
# Script
if __name__=='__main__':
    
    # Logs
    
    # Checking if the log folder exists, if not, it creates
    if not os.path.exists('/Users/nando/Comunidade DS/ds_ao_dev/logs'):
        os.makedirs('/Users/nando/Comunidade DS/ds_ao_dev/logs')
    # Log Configuration
    logging.basicConfig(filename='/Users/nando/Comunidade DS/ds_ao_dev/logs/webscraping.txt',level=logging.DEBUG,format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    
    # Data Collect, Cleaning and Insertion to DB
    
    dados = ProductIDs()
    dados = ProductFeatures(dados)
    dados = DataCleaning(dados)
    DataBase(dados,create_engine('sqlite:///hm_db.sqlite',echo=False))