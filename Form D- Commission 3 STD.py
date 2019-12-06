
# coding: utf-8

# ### This code explores the Form D dataset and focuses on Sales Commission.

# In[128]:

import pandas as pd
get_ipython().magic('matplotlib inline')
from scipy import stats
import matplotlib.pyplot as plt
plt.rcParams["figure.dpi"] = 600
import os
import numpy as np
import warnings
warnings.filterwarnings('ignore')


# In[129]:

os.chdir('J:/Oea/bresler/David/FormD')

# years only starting at 2010 and above based on my Form D downloads.
years = [2010,2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]
quarters = [1,2,3,4]
df_form = pd.DataFrame()
for years in years:
    for quarter in quarters:
        frame = pd.read_csv('Q' + str(quarter) + '_' + str(years) + '_FORMDSUBMISSION.tsv', sep = '\t')
        df_form = df_form.append(frame, ignore_index=True) 
    


# In[130]:

df_form = df_form.drop(['FILE_NUM', 'SCHEMAVERSION', 'TESTORLIVE', 'OVER100PERSONSFLAG', 'OVER100ISSUERFLAG'], axis=1)


# In[131]:

# years only starting at 2010 and above based on my Form D downloads.
years = [2010,2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]
quarters = [1,2,3,4]
df_issuers = pd.DataFrame()
for years in years:
    for quarter in quarters:
        frame = pd.read_csv('Q' + str(quarter) + '_' + str(years) + '_ISSUERS.tsv', sep = '\t')
        df_issuers = df_issuers.append(frame, ignore_index=True) 


# In[132]:

df_issuers = df_issuers.drop([ 'IS_PRIMARYISSUER_FLAG', 'ISSUER_SEQ_KEY', 'STREET1', 'STREET2', 'CITY', 'STATEORCOUNTRYDESCRIPTION', 'ZIPCODE',
                'ISSUER_PREVIOUSNAME_1', 'ISSUER_PREVIOUSNAME_2', 'ISSUER_PREVIOUSNAME_3', 'EDGAR_PREVIOUSNAME_1','EDGAR_PREVIOUSNAME_2',
                'EDGAR_PREVIOUSNAME_3', 'YEAROFINC_TIMESPAN_CHOICE', 'ISSUERPHONENUMBER', 'ENTITYTYPE','ENTITYTYPEOTHERDESC', 'YEAROFINC_VALUE_ENTERED' ], axis=1)


# In[133]:

df_FormD = df_form.merge(df_issuers)


# In[134]:

# years only starting at 2010 and above based on my Form D downloads.
years = [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]
quarters = [1,2,3,4]
df_offerings = pd.DataFrame()
for years in years:
    for quarter in quarters:
        frame = pd.read_csv('Q' + str(quarter) + '_' + str(years) + '_OFFERING.tsv', sep = '\t')
        df_offerings = df_offerings.append(frame, ignore_index=True) 


# In[135]:

df_offerings = df_offerings.drop (['IS40ACT', 'ISAMENDMENT','PREVIOUSACCESSIONNUMBER','YETTOOCCUR','MORETHANONEYEAR', 'ISBUSINESSCOMBINATIONTRANS',
                    'ISSECURITYTOBEACQUIREDTYPE','ISEQUITYTYPE','ISDEBTTYPE','ISOPTIONTOACQUIRETYPE','ISSECURITYTOBEACQUIREDTYPE',
                   'ISPOOLEDINVESTMENTFUNDTYPE','ISTENANTINCOMMONTYPE','ISMINERALPROPERTYTYPE','ISOTHERTYPE','DESCRIPTIONOFOTHERTYPE',
                   'BUSCOMBCLARIFICATIONOFRESP','MINIMUMINVESTMENTACCEPTED','OVER100RECIPIENTFLAG',
                   'SALESAMTCLARIFICATIONOFRESP','HASNONACCREDITEDINVESTORS','NUMBERNONACCREDITEDINVESTORS',
                   'TOTALNUMBERALREADYINVESTED','SALESCOMM_ISESTIMATE','FINDERSFEE_ISESTIMATE',
                   'FINDERFEECLARIFICATIONOFRESP','GROSSPROCEEDSUSED_DOLLARAMOUNT','GROSSPROCEEDSUSED_ISESTIMATE','GROSSPROCEEDSUSED_CLAROFRESP',
                   'AUTHORIZEDREPRESENTATIVE','INVESTMENTFUNDTYPE', 'SALE_DATE' ],axis =1)


# In[136]:

df_FormD = df_FormD.merge(df_offerings)


# In[137]:

df_FormD = df_FormD.drop(['ACCESSIONNUMBER'], axis =1)


# In[138]:

#I renamed the columns to merge into other pieces of code. This line is not needed.
df_FormD = df_FormD.rename(columns={'FILING_DATE':'Date Filed', 'SIC_CODE':'SIC', 'SUBMISSIONTYPE':'Form Type','STATEORCOUNTRY':'State_bus',
                         'JURISDICTIONOFINC':'state_inc','INDUSTRYGROUPTYPE':'Description'})
                         


# In[139]:

df_FormD


# In[141]:

#export_csv = df_FormD.to_excel (r'J:\Oea\bresler\David\df_formD_test.xlsx', header=True)


# In[142]:

entity_counts = df_FormD.groupby(['ENTITYNAME'])['SALESCOMM_DOLLARAMOUNT'].agg(['sum'])


# In[143]:

description_counts = df_FormD.groupby(['Description', 'ENTITYNAME'])['SALESCOMM_DOLLARAMOUNT'].agg(['sum'])


# In[144]:

description_counts = description_counts[description_counts['sum']>0]


# In[145]:

description_counts = description_counts.reset_index()


# In[146]:

#export_csv = description_counts.to_excel (r'J:\Oea\bresler\David\description_counts.xlsx', header=True)


# In[147]:

# This line removes any record outside 3 standard deviations. 
#entity_counts = entity_counts[(np.abs(stats.zscore(entity_counts)) < 3).all(axis=1)]


# In[148]:

entity_counts


# In[149]:

entity_counts = entity_counts.sort(['sum'], ascending = False)


# In[150]:

#export_csv = entity_counts.to_excel (r'J:\Oea\bresler\David\entity_counts.xlsx', header=True)


# In[151]:

top10 = entity_counts[:10]


# In[152]:

top10


# In[153]:

#export_csv = top10.to_excel (r'J:\Oea\bresler\David\top10.xlsx', header=True)


# In[154]:

top10.index


# In[155]:

top10filter_list = top10.index.tolist()


# In[156]:

top10filter_list


# In[157]:

top10.plot(kind = 'barh', )


# In[178]:

fig, ax = plt.subplots(figsize=(16,12))
top10.plot(kind = 'barh',ax=ax, linewidth=1, fontsize=16)

#plt.tight_layout()
plt.legend(loc=0, prop={'size': 20})
plt.title('Highest Sales Commission', fontsize=35)
plt.xlabel('100 Billions', fontsize=20)
plt.ylabel('Industry', fontsize = 20);


# In[158]:

table_fees= pd.pivot_table(df_FormD, values = ['SALESCOMM_DOLLARAMOUNT', 'FINDERSFEE_DOLLARAMOUNT'],
                      index=['Description' ,'ENTITYNAME'],
                     aggfunc= np.sum,
                                        )


# In[159]:

table_fees


# In[160]:

top10fees = pd.DataFrame()


# In[161]:

os.chdir('J:/Oea/bresler/David/')
top10fees = pd.read_csv('top10fees.csv')


# In[162]:

top10fees = top10fees.set_index('Type')


# In[165]:

top10fees.plot(kind = 'barh', )


# In[173]:

fig, ax = plt.subplots(figsize=(16,12))
top10fees.plot(kind = 'barh',ax=ax, linewidth=1, fontsize=20)

#plt.tight_layout()
plt.legend(loc=0, prop={'size': 20})
plt.title('Highest Sales Commission and Fees by Industry', fontsize=35)
plt.xlabel('100 Billions', fontsize=20)
plt.ylabel('Industry', fontsize = 20);

ax.grid(False) #remove grid
#ax.set_xticks([]) #set empty xticks

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



