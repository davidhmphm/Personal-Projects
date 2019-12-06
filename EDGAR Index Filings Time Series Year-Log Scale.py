
# coding: utf-8

# #EDGAR Index Notebook that produces time series graph for any forms. 

# In[1]:

import pandas as pd
get_ipython().magic('matplotlib inline')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
plt.rcParams["figure.dpi"] = 360
import os
import numpy as np
import warnings
import datetime
warnings.filterwarnings('ignore')


# In[2]:

form_list=['S-1','S-3','S-4','F-1','F-3','F-4','D','C', '1-A']


# In[3]:

#Pulls data from the idx master file.
os.chdir('J:/Oea/bresler/David/Index')

# Years can go back all the way to 1993.
years = [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]
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


# In[4]:

dfmerge


# In[5]:

dfmerge['Date Filed'] = pd.to_datetime(dfmerge['Date Filed'])


# In[6]:

df_filtered=dfmerge[dfmerge['Form Type'].isin(form_list)]


# In[7]:

grouped_counts = df_filtered.groupby(['Form Type', 'Date Filed'])['Form Type'].value_counts().reset_index(name= 'Counts')


# In[8]:

dfyear = grouped_counts


# In[9]:

dfyear['year'] = pd.DatetimeIndex(dfyear['Date Filed']).year
dfyear['month'] = pd.DatetimeIndex(grouped_counts['Date Filed']).month


# In[10]:

dfyear


# In[11]:

dfyear = dfyear.groupby(['year', 'Form Type'])['Counts'].agg([sum]).unstack()


# In[12]:

dfyear


# In[13]:

# This block is needed for formatting purposes. Remove this if using other forms.
dfyear.T.reset_index(drop=True).T
dfyear.columns = ['1-A', 'C', 'D', 'F-1', 'F-3', 'F-4', 'S-1', 'S-3', 'S-4']


# In[14]:

fig, ax = plt.subplots(figsize=(14,12))
dfyear.plot(ax=ax, logy =True, colormap = 'spectral', linewidth=5, fontsize=20)
#Remove x tick labels if using other years
x_ticks_labels = ['2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019']
ax.set_xticklabels(x_ticks_labels, rotation='vertical', fontsize=18)

plt.tight_layout()
plt.axis('equal')
plt.title('Capital Raising Forms by Year', fontsize=30)
plt.xlabel('Year', fontsize=20)
plt.ylabel('Sum per Year', fontsize = 20);
fig.autofmt_xdate()

ax.set_ylim([10.0, 1000000.0])
ax.set_xlim([2011.0, 2019.0])
plt.show();

