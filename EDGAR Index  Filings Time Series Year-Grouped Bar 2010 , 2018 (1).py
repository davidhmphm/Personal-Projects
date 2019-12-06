
# coding: utf-8

# In[2]:

import pandas as pd
get_ipython().magic('matplotlib inline')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
plt.rcParams["figure.dpi"] = 360 #Quality of the graphs
import os
import numpy as np
import datetime


# In[3]:

form_list=['S-1','S-3','S-4','F-1','F-3','F-4','D','C','1-A']


# In[4]:

#Pulls data from the idx master file.
os.chdir('J:/Oea/bresler/David/Index')

years = [2010,2018]
quarters = [1,2,3,4]
dfmerge = pd.DataFrame()
for years in years:
    for quarter in quarters:
        frame = pd.read_csv('Q' + str(quarter) + '_' + str(years) +'.idx',error_bad_lines = False, sep = '|',  encoding='latin-1')
        frame['Company Name'] = frame['Company Name'].replace({',':'', '\.':'', '"':''}, regex=True)
        dfmerge = dfmerge.append(frame, ignore_index=True) 
        dfmerge['Company Name'] = dfmerge['Company Name'].astype(str)
        dfmerge['Company Name'] = dfmerge['Company Name'].map(lambda x: x.strip())
        dfmerge['CIK'] = dfmerge['CIK'].astype(str)
        dfmerge['CIK'] = dfmerge['CIK'].map(lambda x: x.strip())
        dfmerge = dfmerge[(dfmerge['Company Name'] != 'blank')]
        dfmerge['CIK'] = dfmerge['CIK'].map(lambda x: x.lstrip('0'))
        dfmerge['CIK'] = dfmerge['CIK'].astype(int)
        dfmerge['Date Filed'] = dfmerge['Date Filed'].astype(str)
        dfmerge['Date Filed'] = dfmerge['Date Filed'].map(lambda x: x.strip())
        dfmerge['Filename'] = dfmerge['Filename'].astype(str)
        dfmerge['Filename'] = dfmerge['Filename'].map(lambda x: x.lstrip('edgar/data/'))
        dfmerge['Filename'] = dfmerge['Filename'].map(lambda x: x.rstrip('.txt'))
        dfmerge['Filename'] = dfmerge['Filename'].str.replace(r'^[\d]*\/', '')


# In[5]:

dfmerge


# In[6]:

dfmerge['Date Filed'] = pd.to_datetime(dfmerge['Date Filed'])


# In[7]:

df_filtered=dfmerge[dfmerge['Form Type'].isin(form_list)]


# In[8]:

grouped_counts = df_filtered.groupby(['Form Type', 'Date Filed'])['Form Type'].value_counts().reset_index(name= 'Counts')


# In[9]:

grouped_counts


# In[10]:

grouped_counts.drop(['level_2'], axis =1 , inplace=True)


# In[11]:

dfyear = grouped_counts


# In[12]:

dfyear['year'] = pd.DatetimeIndex(dfyear['Date Filed']).year
dfyear['month'] = pd.DatetimeIndex(grouped_counts['Date Filed']).month


# In[13]:

dfyear


# In[14]:

dfyear = dfyear.groupby(['year', 'Form Type'])['Counts'].agg([sum]).unstack()


# In[15]:

dfyear.T.reset_index(drop=True).T # This hardcoded part is needed for formatting purposes for the graph.
dfyear.columns = ['1-A', 'C', 'D', 'F-1', 'F-3', 'F-4', 'S-1', 'S-3', 'S-4']


# In[16]:

dfyeart = dfyear.T


# In[17]:

fig, ax = plt.subplots(figsize=(18,12))
dfyeart.plot(kind = 'barh',ax=ax, linewidth=1, fontsize=25)

#plt.tight_layout()
plt.legend(loc=1, prop={'size': 20})
plt.title('Difference of Forms, 2010 to 2018', fontsize=35)
plt.xlabel('Sum', fontsize=20)
plt.ylabel('Form Type', fontsize = 20);

ax.grid(False) #remove grid
ax.set_xticks([]) #set empty xticks

totals = []

# find the values and append to list
for i in ax.patches:
    totals.append(i.get_width())

# set individual bar labels using above list
total = sum(totals)

# set individual bar labels using above list
for i in ax.patches:
    # get_width pulls left or right; get_y pushes up or down
    ax.text(i.get_width()+250, i.get_y()+0.05,             str((i.get_width())), fontsize=12,
color='black')
plt.show();


# In[ ]:



