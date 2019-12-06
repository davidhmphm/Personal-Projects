
# coding: utf-8

# #Table of Contents
# 
# 1. [Introduction](#intro)
#     - 1.1 [Import Statements - Special Notes](#import)
#     - 1.2 [Change Forms Here](#forms)
# 2. [Data](#data)
#     - 2.1 [Input Master IDX](#inputidx)
#     - 2.2 [Input SRC data](#inputsrc)
#     - 2.3 [Data Cleaning: Filer Status](#datacleaning)
#     - 2.4 [Adding Sector Codes to the Data](#sectorcodes)
#     - 2.5 [Filtering the IDX](#filtering)
# 3. [Descriptive Statistics](#stats)
#     - 3.1 [Resetting Index, Data Manipulation](#resetting)
#     - 3.2 [Getting Sum, Nanmean, Median](#gettingsum)
#     - 3.3 [Getting the Mean](#gettingmean)
#     - 3.4 [Merging](#merging)
#     - 3.5 [Adding Percentage](#percentage)
#     - 3.6 [Formatting and Cleaning](#formatting)
# 4. [Visualization and Plotting](#vis)
#     - 4.1

# <a id='intro'><a>

# # 1. Introduction
# 
# This particular notebook focuses on combining sector codes for the entire EDGAR filings while getting statistical descriptions of the dataset such as mean, median, and sum. This notebook is useful for data exploration of selected forms with graphs and tables ready for production.
# 
# An important note is that I used 10-K information to supplement the SIC and Sector Codes which means that the farther you go away from public companies, the less accurate the sector codes would be. 

# <a id='import'></a>

# ### 1.1 Import Statements - Special Notes
# 
# We import warnings in the statements because there will be warnings because there are several functions that deal with NaN tuples. The warnings themselves cause the query to slow down significantly, so we remove them here.

# In[1]:

import pandas as pd
get_ipython().magic('matplotlib inline')
import matplotlib.pyplot as plt
plt.rcParams["figure.dpi"] = 360
import os
import numpy as np
import warnings
warnings.filterwarnings('ignore')


# <a id='forms'></a>

# ### 1.2 Change Forms Here
# 
# If you want to filter the EDGAR forms, please change it below

# In[2]:

form_list=['10-K']


# In[3]:

#form_list=['S-1','S-3','S-4','F-1','F-3','F-4','D','D/A','144']


# <a id='data'></a>

# # 2. Data

# <a id='inputidx'></a>

# ## 2.1 Input: Master IDX
# 
# The code below pulls from each quarter of the 2018 index files and combines them into one dataframe. We use the quarter index files to be flexible in the code in case we want to filter down by quarters.

# In[4]:

#Pulls data from the idx master file.
os.chdir('J:/Oea/bresler/David/Index')

quarters = [1,2,3,4]
dfmerge = pd.DataFrame()
for quarter in quarters:
    frame = pd.read_csv('Q' + str(quarter) + '_2018'  +'.idx',error_bad_lines = False, sep = '|')
    frame['Company Name'] = frame['Company Name'].replace({',':'', '\.':''}, regex=True)
    dfmerge = dfmerge.append(frame, ignore_index=True) 
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


# #### Table 2.1 Master IDX

# In[5]:

dfmerge


# In[6]:

#export_csv = test.to_excel (r'J:\Oea\bresler\David\test.xlsx', header=True)


# <a id='inputsrc'></a>

# ### 2.2 Input: Custom SRC data
# 
# The code below pulls in an EDGAR scrape of 10-K's and pulls additional information such as filing status (src, egc) or state of business (CA, NY).
# It also formats the data types for the information.

# In[7]:

# Pulls data from src_data_q1,2,3,4, and reads data to a dataframe.
os.chdir('J:/Oea/bresler/')
dfparsed = pd.DataFrame()
for quarter in quarters:
    frame = pd.read_csv('src_data_q' + str(quarter) + '.csv', encoding='latin1', skipinitialspace = True, delimiter = ',')
    dfparsed = dfparsed.append(frame, ignore_index=True)
dfparsed['Company'] = dfparsed['Company'].map(lambda x: x.strip())
dfparsed['CIK'] = dfparsed['CIK'].astype(str)
dfparsed['CIK'] = dfparsed['CIK'].map(lambda x: x.strip())
dfparsed = dfparsed[(dfparsed['Company'] != 'blank')]
dfparsed['CIK'] = dfparsed['CIK'].map(lambda x: x.lstrip('0'))
dfparsed['CIK'] = dfparsed['CIK'].astype(int)
dfparsed['Filedate'] = dfparsed['Filedate'].astype(str)
dfparsed['Filedate'] = dfparsed['Filedate'].map(lambda x: x.strip())
dfparsed['Filename'] = dfparsed['Filename'].map(lambda x: x.rstrip('.txt'))
dfparsed['FORM'] = dfparsed['FORM'].map(lambda x: x.strip())
dfparsed['State_bus'] = dfparsed['State_bus'].map(lambda x: x.strip())
dfparsed['state_inc'] = dfparsed['state_inc'].map(lambda x: x.strip())
dfparsed2 = dfparsed[(dfparsed['FORM'] == '10-K') | (dfparsed['FORM'] == '10-K/A')]


# #### Table 2.2 EDGAR scrape of 10-K
# 
# Our dataframe as the filing status coded as boolean values and the pulled text of the filing status.

# In[8]:

dfparsed


# <a id='datacleaning'></a>

# ### 2.3 Data Cleaning: Filer_Status
# 
# We want to combine all of the boolean filer status columns into one and drop the previous columns. We do this by flash filling foward and merging the dataframes together.

# In[9]:

#rename columns to match first dataframe for merge
dfparsed['Company Name']=dfparsed['Company']
dfparsed['Date Filed']=dfparsed['Filedate']
dfparsed['Form Type']=dfparsed['FORM']
#drop dup lines for filer status
dfparsed= dfparsed.drop(['search_line', 'search_line3','search_line_af','search_line_egc', 'search_line_laf',
                                         'search_line_naf', 'search_line_src', 'search_line_wksi', 'search_line_wksi_yes', 
                                          'wksi','Company','Filedate','FORM' ], axis=1)
#change 1's to string
dfparsed['src'] = dfparsed['src'].replace('1', 'src')
dfparsed['egc'] = dfparsed['egc'].replace('1', 'egc')
dfparsed['laf'] = dfparsed['laf'].replace('1', 'laf')
dfparsed['af'] = dfparsed['af'].replace('1', 'af')
dfparsed['naf'] = dfparsed['naf'].replace('1', 'naf')
#replaces int 0's to blank
dfparsed['src'] = dfparsed['src'].replace(0, np.nan)
dfparsed['egc'] = dfparsed['egc'].replace(0, np.nan)
dfparsed['laf'] = dfparsed['laf'].replace(0, np.nan)
dfparsed['af'] = dfparsed['af'].replace(0, np.nan)
dfparsed['naf'] = dfparsed['naf'].replace(0, np.nan)
#forward fill the columns and drop https cells
dfparsed['Filer_Type'] = dfparsed.ffill(axis=1).loc[:, 'src']
dfparsed['Filer_Type']=dfparsed['Filer_Type'].replace(r'^https.*$',np.nan, regex=True)
# drop individual filer status lines
dfparsed= dfparsed.drop(['af','egc','naf','src','laf'], axis=1)
#merge both dataframes together
dfmaster= pd.merge(dfmerge, dfparsed, how='left',on=['CIK'])
#drop additional fields
dfmaster= dfmaster.drop(['Company Name_y', 'Date Filed_y','Form Type_y','Filename_y' ], axis=1)
dfmaster=dfmaster.rename(columns={'Company Name_x': 'Company Name', 'Form Type_x': 'Form Type', 
                         'Date Filed_x': 'Date Filed', 'Filename_x': 'Filename'}, errors='raise')


# In[10]:

dfmaster


# #### Table 2.3 IDX with filer_status and other information

# In[11]:

dfmaster


# In[12]:

#dfmaster = pd.concat([dfmaster, df_formd], join ="outer")


# In[13]:

dfmaster


# <a id='sectorcodes'></a>

# ### 2.4 Adding Sector Codes to the data
# 
# The sector codes are the larger industry codes for the SIC that includes the description of the larger industry. We read in a csv with the codes and merge the two dataframes together.

# In[14]:

#import SIC Codes
os.chdir('J:/Oea/bresler/David/')
df_code = pd.DataFrame()
frame = pd.read_csv('SIC Codes.csv',error_bad_lines = False)
df_code= frame
results= pd.merge(dfmaster, df_code, how='outer', on=['SIC'])
df_sector = pd.DataFrame()
frame = pd.read_excel('Sector Code.xlsx', converters={'Sector Code':str, 'Description':str})
df_sector = frame


# #### Table 2.4 Sector Codes

# In[15]:

df_sector


# In[16]:

#Add in Sector by slicing the SIC code to 2 digits and merge and fix formatting
results['Sector Code']=results['SIC']
results['Sector Code']=results['Sector Code'].astype(str).str.slice(0, 2)
results['Sector Code']=results['Sector Code'].replace(r'^na$',np.nan, regex=True).replace('',np.nan)
results2=results.merge(df_sector, on='Sector Code', how='left')


# #### Table 2.4 Master IDX with Sector Codes

# In[17]:

results2


# <a id='filtering'></a>

# ### 2.5 Filtering the IDX
# 
# Since we're focusing on forms have some association to raising captial, we filter the forms below.

# In[18]:

df_filtered=results2[results2['Form Type'].isin(form_list)]
df_filtered=df_filtered.fillna('N/A')


# #### Table 2.5 Filtered IDX
# 
# If you want to work with filtered data, start here.

# In[19]:

df_filtered


# <a id='stats'></a>

# ## 3. Descriptive Statistics

# <a id='resetting'></a>

# ### 3.1 Resetting Index, Data Manipulation
# 
# Since we want to get a count of all the forms, we need to reset the index and create a new column for counts. If we don't do that, we run into errors with string manipulation. The end goal this section is to get standard descriptive statistics for the form types by SIC and Sector.

# In[20]:

grouped_counts = df_filtered.groupby(['Description', 'SIC', 'CIK'])['Form Type'].value_counts(dropna=True).reset_index(name='counts')
grouped_counts =grouped_counts.rename(columns={'level_3':'Form Type'})


# In[21]:

grouped_counts_business = df_filtered.groupby(['Description', 'State_bus', 'CIK'])['Form Type'].value_counts().reset_index(name='counts')
grouped_counts_business =grouped_counts_business.rename(columns={'level_3':'Form Type'})


# <a id='gettingsum'></a>

# ### 3.2 Getting Sum, Median, and Nanmean
# 
# We replace the 0's with nan to get the nanmean by stacking and resetting the index and doing another groupby. I won't expand the tables until later, but feel free to add print statements.

# In[22]:

table_sector_median = pd.pivot_table(grouped_counts, columns = ['Form Type'],
                                                     index=['Description', 'SIC', 'CIK'],
                                                     fill_value =0,
                                                     aggfunc= np.sum,
                                     )
table_sector_median = table_sector_median.stack().reset_index().replace(0, np.nan)
table_SIC = table_sector_median.groupby(['Description', 'SIC', 'Form Type'])['counts'].agg([sum,'median', np.nanmean,])


# In[23]:

table_business_median_CIK = pd.pivot_table(grouped_counts_business,
                                                     columns = ['Form Type'],
                                                     index=['Description', 'State_bus', 'CIK'],
                                                     fill_value =0,
                                                     aggfunc= np.sum,
                                     )
table_business_median_CIK = table_business_median_CIK.stack().reset_index().replace(0, np.nan)
table_business = table_business_median_CIK.groupby(['Description', 'State_bus', 'Form Type'])['counts'].agg([sum,'median', np.nanmean,])


# In[24]:

table= pd.pivot_table(grouped_counts, columns = ['Form Type'],
                      index=['Description', 'SIC', 'CIK'],
                      fill_value =0,
                     aggfunc= np.sum,                     
                     )
table= table.stack().reset_index()
table=table.groupby(['Description', 'Form Type'])['counts'].agg([sum, np.mean])
table_sector_median = table_sector_median.groupby(['Description', 'Form Type'])['counts'].agg([sum,'median', np.nanmean,])


# In[25]:

table_business_stat= pd.pivot_table(grouped_counts_business, columns = ['Form Type'],
                      index=['Description', 'State_bus', 'CIK'],
                      fill_value =0,
                     aggfunc= np.sum,                     
                     )
table_business_stat= table_business_stat.stack().reset_index()
table_business_stat=table_business_stat.groupby(['State_bus', 'Form Type'])['counts'].agg([sum, np.mean])
table_business_median_CIK = table_business_median_CIK.groupby(['State_bus', 'Form Type'])['counts'].agg([sum,'median', np.nanmean,])


# ####Table 3.2 SIC with sum, median, nanmean

# In[26]:

table_SIC


# In[27]:

table_business


# <a id='gettingmean'></a>

# ### 3.3 Getting Mean
# 
# We also want to find the mean (to include zeroes).

# In[28]:

table_SIC_mean= pd.pivot_table(grouped_counts, columns = ['Form Type'],
                      index=['Description', 'SIC', 'CIK'],
                      fill_value =0,
                     aggfunc= np.sum,
                                        )

table_SIC_mean= table_SIC_mean.stack()
table_SIC_mean=table_SIC_mean.reset_index()
table_SIC_mean=table_SIC_mean.groupby(['Description', 'SIC', 'Form Type'])['counts'].agg([sum, np.mean])


# In[29]:

table_business_mean= pd.pivot_table(grouped_counts_business, columns = ['Form Type'],
                      index=['Description', 'State_bus', 'CIK'],
                      fill_value =0,
                     aggfunc= np.sum,
                                        )

table_business_mean= table_business_mean.stack()
table_business_mean=table_business_mean.reset_index()
table_business_mean=table_business_mean.groupby(['Description', 'State_bus', 'Form Type'])['counts'].agg([sum, np.mean])


# ####Table 3.3 SIC with mean

# In[30]:

table_SIC_mean


# <a id='merging'></a>

# ### 3.4 Merging and Formatting
# 
# Once we have the two dataframes, we merge the frames together and we restack the columns to get Form Types as the first level.

# In[31]:

table_business = table_business.merge(table_business_mean, how ='outer', left_index= True, right_index = True)
#drop dup sum
table_business = table_business.drop(['sum_y'], axis =1)
#re stack
table_business=table_business.unstack(level = 2).stack(level = 0).unstack(level = 2).rename(columns={'sum_x':'Sum'})


# In[32]:

table_SIC = table_SIC.merge(table_SIC_mean, how ='outer', left_index= True, right_index = True)
#drop dup sum
table_SIC = table_SIC.drop(['sum_y'], axis =1)
#re stack
table_SIC=table_SIC.unstack(level = 2).stack(level = 0).unstack(level = 2).rename(columns={'sum_x':'Sum'})


# In[33]:

table_business_stat = table_business_median_CIK.merge(table_business_stat, how ='inner', left_index= True, right_index = True)
#drop duplicate sum
table_business_stat = table_business_stat.drop(['sum_y'], axis =1)
#re stacking to place Form Type on top
table_business_stat =table_business_stat.unstack(level =1).stack(level = 0).unstack(level =1)


# In[34]:

table_sector = table_sector_median.merge(table, how ='inner', left_index= True, right_index = True)
#drop duplicate sum
table_sector = table_sector.drop(['sum_y'], axis =1)
#re stacking to place Form Type on top
table_sector =table_sector.unstack(level =1).stack(level = 0).unstack(level =1)


# #### Table 3.4 Form Type by SIC

# In[35]:

table_SIC


# <a id='percentage'></a>

# ### 3.5 Adding Percentage
# 
# Now we want to figure out what percent each SIC adds to the total forms. We do this by resetting the index again and doing another groupby.

# In[36]:

table_business_percent = table_business.stack(level =0).reset_index()
table_business_percent = table_business_percent.groupby(['Description','State_bus', 'Form Type'])['Sum'].agg([sum])
table_business_percent = table_business_percent.stack(level =0).reset_index()
table_business_percent = table_business_percent.drop(['level_3'], axis=1)
table_business_percent = table_business_percent.groupby(['Description', 'State_bus', 'Form Type'])[0].agg([sum])
table_business_percent = table_business_percent.groupby(level=2).transform(lambda x: x/x.sum())
table_business_percent = table_business_percent.rename(columns={'sum':'Sector%'})
table_business_percent = table_business_percent.unstack(level =2).stack(level = 0).unstack(level =2)
table_business_percent = pd.concat([table_business, table_business_percent], axis=1)
table_business= table_business.unstack(level =1).stack(level = 0).unstack(level =1)


# In[37]:

table_SIC_percent = table_SIC.stack(level =0).reset_index()
table_SIC_percent = table_SIC_percent.groupby(['Description','SIC', 'Form Type'])['Sum'].agg([sum])
table_SIC_percent = table_SIC_percent.stack(level =0).reset_index()
table_SIC_percent = table_SIC_percent.drop(['level_3'], axis=1)
table_SIC_percent = table_SIC_percent.groupby(['Description', 'SIC', 'Form Type'])[0].agg([sum])
table_SIC_percent = table_SIC_percent.groupby(level=2).transform(lambda x: x/x.sum())
table_SIC_percent = table_SIC_percent.rename(columns={'sum':'Sector%'})
table_SIC_percent = table_SIC_percent.unstack(level =2).stack(level = 0).unstack(level =2)
table_SIC = pd.concat([table_SIC, table_SIC_percent], axis=1)
table_SIC = table_SIC.stack().unstack()


# In[38]:

table_sector_percent = table_sector.stack(level =0).reset_index()
table_sector_percent = table_sector_percent.groupby(['Description', 'Form Type'])['sum_x'].agg([sum])
table_sector_percent = table_sector_percent.groupby(level=1).transform(lambda x: x/x.sum())
table_sector_percent = table_sector_percent.rename(columns={'sum':'Sector%'})
table_sector_percent = table_sector_percent.unstack(level =1).stack(level = 0).unstack(level =1)
table_sector = pd.concat([table_sector, table_sector_percent], axis=1)
table_sector = table_sector.stack().unstack().rename(columns={'sum_x':'Sum'})


# In[39]:

table_business_stat_percent = table_business_stat.stack(level =0).reset_index()
table_business_stat_percent = table_business_stat_percent.groupby(['State_bus', 'Form Type'])['sum_x'].agg([sum])
table_business_stat_percent = table_business_stat_percent.groupby(level=1).transform(lambda x: x/x.sum())
table_business_stat_percent = table_business_stat_percent.rename(columns={'sum':'Sector%'})
table_business_stat_percent = table_business_stat_percent.unstack(level =1).stack(level = 0).unstack(level =1)
table_business_stat = pd.concat([table_business_stat, table_business_stat_percent], axis=1)
table_business_stat = table_business_stat.stack().unstack().rename(columns={'sum_x':'Sum'})


# #### Table 3.5 Form Type by SIC with Percentages

# In[40]:

table_SIC


# ####Table 3.51 Form Type by Sector with Percentages

# In[41]:

table_sector


# In[42]:

table_business_stat


# <a id='formatting'></a>

# ### 3.6 Formatting and Cleaning
# 
# We want to remove the nan values and round the float data types. We also want to add the % sign to the percentage.

# In[43]:

table_SIC = table_SIC.sort_index(axis=1).stack(level =0)
table_SIC['mean'] = table_SIC['mean'].map('{:.2f}'.format)
table_SIC['nanmean'] = table_SIC['nanmean'].map('{:.2f}'.format)
table_SIC['Sector%'] = table_SIC['Sector%'].map('{:.2%}'.format)
table_SIC = table_SIC.fillna('')
table_SIC['Sector%'] = table_SIC['Sector%'].replace('nan%', '')
table_SIC['nanmean'] = table_SIC['nanmean'].replace('nan', '')
table_SIC = table_SIC.unstack(level = 2).stack(level = 0).unstack(level = 2)


# In[44]:

table_sector = table_sector.sort_index(axis=1).stack(level =0)
table_sector['Sum'] = table_sector['Sum'].fillna(0).astype(int)
table_sector['mean'] = table_sector['mean'].map('{:.2f}'.format)
table_sector['nanmean'] = table_sector['nanmean'].map('{:.2f}'.format)
table_sector['median'] = table_sector['median'].map('{:.0f}'.format)
table_sector['Sector%'] = table_sector['Sector%'].map('{:.2%}'.format)
table_sector = table_sector.fillna('')
table_sector['Sector%'] = table_sector['Sector%'].replace('nan%', '')
table_sector['nanmean'] = table_sector['nanmean'].replace('nan', '')
table_sector['mean'] = table_sector['mean'].replace('nan', '')
table_sector['median'] = table_sector['median'].replace('nan', '')
table_sector=table_sector.unstack(level = 1).stack(level = 0).unstack(level =1)


# In[45]:

table_business_stat = table_business_stat.sort_index(axis=1).stack(level =0)
table_business_stat['Sum'] = table_business_stat['Sum'].fillna(0).astype(int)
table_business_stat['mean'] = table_business_stat['mean'].map('{:.2f}'.format)
table_business_stat['nanmean'] = table_business_stat['nanmean'].map('{:.2f}'.format)
table_business_stat['median'] = table_business_stat['median'].map('{:.0f}'.format)
table_business_stat['Sector%'] = table_business_stat['Sector%'].map('{:.2%}'.format)
table_business_stat = table_business_stat.fillna('')
table_business_stat['Sector%'] = table_business_stat['Sector%'].replace('nan%', '')
table_business_stat['nanmean'] = table_business_stat['nanmean'].replace('nan', '')
table_business_stat['mean'] = table_business_stat['mean'].replace('nan', '')
table_business_stat['median'] = table_business_stat['median'].replace('nan', '')
table_business_stat = table_business_stat.unstack(level = 1).stack(level = 0).unstack(level =1)


# #### Table 3.6 Form Type by SIC, Formatted

# In[46]:

table_SIC


# In[47]:

#export_csv = table_SIC.to_excel (r'J:\Oea\bresler\David\grouped_counts_stats_SIC_w_Percentage.xlsx', header=True)


# #### Table 3.61 Form Type by Sector, Formatted

# In[48]:

table_sector


# In[49]:

#export_csv = table_sector.to_excel (r'J:\Oea\bresler\David\grouped_counts_stats_Sector_w_Percentage.xlsx', header=True)


# <a id='vis'><a>

# In[50]:

table_business_stat


# In[51]:

#export_csv = table_business_stat.to_excel (r'J:\Oea\bresler\David\grouped_counts_stats_State_bus.xlsx', header=True)


# <a id='vis'></a>

# ## 4. Visualization and Plotting

# ### 4.1 H Bar Graph by Sector

# In[52]:

table_sector.sortlevel(axis = 1,  ascending=False).plot(kind='barh', stacked=True, subplots=False, sort_columns = True, figsize =(14,10))


# ###4.2 H Bar Graph by State_Bus
# 

# In[53]:

table_business_stat.plot(kind='barh', stacked=True, subplots=False, sort_columns = True, figsize =(14,30))


# In[ ]:



