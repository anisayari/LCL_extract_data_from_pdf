import tabula
from os import listdir
from os.path import isfile, join
import os
import pandas as pd
from tqdm import tqdm

mypath = './input_pdf/'
onlyfiles = [f for f in tqdm(listdir(mypath)) if isfile(join(mypath, f))]
print(onlyfiles)

dfs_clean = []
for file in tqdm(onlyfiles):
    path = os.path.join('./input_pdf/', file)
    dfs = tabula.read_pdf(path, stream=True,multiple_tables=True, pages="all",)
    for df in dfs:
        df.columns = df.iloc[0]
        df = df[1:]
        print(df.columns)
        for col in ['DATE LIBELLE','VALEUR','DEBIT','CREDIT']:
            if col not in df.columns :
                print('[ERROR] COLUMN')
                if col == 'DATE LIBELLE' and 'VALEUR' in df.columns:
                    name_date_to_clean = ''
                    for col in df.columns:
                        if type(col) == str:
                            if 'DATE' in col:
                                name_date_to_clean = col
                    if name_date_to_clean != '':
                        df[name_date_to_clean] = df[name_date_to_clean].astype(str)
                        df.LIBELLE.fillna('', inplace=True)
                        df['LIBELLE'] =  df['LIBELLE'].astype(str)
                        df['DATE LIBELLE'] = df[name_date_to_clean] + df['LIBELLE']
                        df.drop(['LIBELLE', name_date_to_clean], axis=1, inplace=True)
        #df = df.dropna(axis=1, how='all')
        if 'VALEUR' not in df.columns:
            continue
        df = df[['DATE LIBELLE','VALEUR','DEBIT','CREDIT']]
        df = df.dropna(subset=['VALEUR'])
        #print(df.columns)
        dfs_clean.append(df)

df = pd.concat(dfs_clean,ignore_index=True)
#df['DEBIT'] = df['DEBIT'].replace('.')
df['DEBIT'] = df['DEBIT'].str.replace(',','.')
df['DEBIT'] = df['DEBIT'].str.replace(' ','')
df['DEBIT'] = pd.to_numeric(df['DEBIT'], errors='coerce')

#print(df['DEBIT'])
#df['DEBIT'] = df['DEBIT'].astype(float)

df['CREDIT'] = df['CREDIT'].str.replace(',','.')
df['CREDIT'] = df['CREDIT'].str.replace(' ','')
#df['CREDIT'] = df['CREDIT'].astype(float)
df['CREDIT'] = pd.to_numeric(df['CREDIT'], errors='coerce')
df['VALEUR'] = pd.to_datetime(df['VALEUR'], format="%d.%m.%y")

df = pd.concat(dfs_clean,ignore_index=True)
df.to_csv('data.csv', index=False)