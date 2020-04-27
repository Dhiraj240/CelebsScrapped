# -*- coding: utf-8 -*-
"""
Created on Mon Apr 27 23:18:59 2020

author: Dhiraj Sharma
"""

import requests
import pandas as pd
from IPython.core.display import HTML
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import re
import pymysql

url = 'https://www.imdb.com/list/ls020280202/'
data = requests.get(url)
bs  = BeautifulSoup(data.text ,  'html.parser')

Actor_Name = []
for div in bs.findAll('div'  , attrs = {'class' : 'lister-item-content'}):
    Actor_Name.append(div.find('a').contents[0])
    
Actor_Images = []
for image in bs.findAll('img' , {'src':re.compile('.jpg')}):
    Actor_Images.append(str(image['src']))




url = 'https://www.imdb.com/list/ls069887650/'
data = requests.get(url)
bs1 = BeautifulSoup(data.text ,  'html.parser')

Actress_Name = []
for div in bs1.findAll('div'  , attrs = {'class' : 'lister-item-content'}):
    Actress_Name.append(div.find('a').contents[0])
    
Actress_Images=[]
for image in bs1.findAll('img' , {'src':re.compile('.jpg')}):
        Actress_Images.append(str(image['src']))

        
Actors=[]   
for i in Actor_Name:
    Actors.append(i.replace('\n' , '').strip())
    
Actress=[]   
for i in Actress_Name:
    Actress.append(i.replace('\n' , '').strip())
    
Celebrities_Names = Actors + Actress  
Celebrities_Images = Actor_Images + Actress_Images 

df = pd.DataFrame()
celebinfo = [] 

for i in Celebrities_Names:

    url = "https://en.wikipedia.org/wiki/{}".format(i)
    data = requests.get(url)
    info = BeautifulSoup(data.text , 'html.parser')
    text = ''
    i=0
    for i in info.find_all('p'):
    
        if len(i.text)>100: 
    
            if len(text)< 1000:
        
                text +=(i.text)
            if len(text)> 1000:
                break

    text = text.replace('\n' , '')
    celebinfo.append(text)
df['Image'] = Celebrities_Images
df['Name']=Celebrities_Names
df['Personality']=celebinfo


# For CSV Dataset
df.to_csv('celebinfo.csv',index=False)

# For SQL Dataset

tableName   = "celebinfo"
dataFrame   = pd.DataFrame(data=df)           
sqlEngine  = create_engine('mysql+pymysql://root:@127.0.0.1/celebinfo', pool_recycle=3600)
dbConnection = sqlEngine.connect()
try:
    frame = dataFrame.to_sql(tableName, dbConnection, if_exists='fail')
except ValueError as x:

    print(x)

except Exception as y:   

    print(y)

else:

    print("Table %s created successfully."%tableName);   

finally:

    dbConnection.close()


