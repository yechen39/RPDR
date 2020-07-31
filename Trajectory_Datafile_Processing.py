import sys
import argparse
import datetime
import os
import timeit
import itertools
import pandas as pd
import numpy as np


from functools import reduce

base_pth="/Volumes/LaCie/PBB/"
fl_nm="Phy_filelist"

filein = base_pth+fl_nm
fileout = '/Users/yc794/Testout.txt'

#parser = argparse.ArgumentParser(description='A filelist of HLA HIBAG calls, one filename per row no quote marks')
#parser.add_argument('-F', required=True, help='full path to the filelist.txt')
#parser.add_argument('-O', required=True, help='full path to the output file')
#args=parser.parse_args()
#filein = args.F
#fileout = args.O



def parse_filelist(pathlist):
    filelist = []
    with open(pathlist, 'r') as f:
        for line in f:
            parse_line = line.strip()
            filelist.append(parse_line)
    return filelist

def read_data(filelist):
    n = 0
    data = []
    header = []
    for file in filelist:
        with open(file, 'r') as f:
            for line in f:
                if n == 0:
                    parse_line = line.strip().split('|')
                    header = parse_line
                n += 1
                if 'EMPI' not in line:
                    parse_line = line.strip().split('|')
                    if parse_line[5] == 'BMI':
                        data.append(parse_line)
                    # Concept_Name is parse_line[5]
    return data, header

def lst_2_pd(lst,header):
    pd_dat = pd.DataFrame(lst, columns=header)
    return pd_dat


# dtype of all columns in df are object, convert data into desired format
def convert_date(pd_dat):
    pd_dat['Date'] = pd_dat['Date'].apply(lambda x: datetime.datetime.strptime(str(x), '%m/%d/%Y').strftime('%m/%d/%Y'))
#    pd_dat['Result'] = pd_dat['Result'].apply(lambda x: float(x))
    return pd_dat


#def item_out(data):
#    _1, _2, item, _3 = read_data(data)
#    with open(fileout, 'w') as f:
#        for i in item:
#            item.write(i+'\n')
#    f.close()

files = parse_filelist(filein)
dat, header= read_data(files)
df = lst_2_pd(dat, header)

df['Date'] = df['Date'].apply(lambda x: datetime.datetime.strptime(str(x), '%m/%d/%Y').strftime('%m/%d/%Y'))
df['Result'] = df['Result'].apply(lambda x: float(x))

df['year'] = pd.DatetimeIndex(df['Date']).year
df = df[df.year >= 2000]
df['period'] = df['year'].apply(lambda x: int(np.ceil((x-2000)/2)))
df['period'] = df['period'].apply(lambda x: 1 if x == 0 else x)

df_mean = df['Result'].groupby([df['EMPI'], df['period']]).mean().reset_index()
df_mean['period_name'] = df_mean['period'].apply(lambda x: 'Period_'+str(x))
df_mean['Attr'] = 'Mean'
df_min = df['Result'].groupby([df['EMPI'], df['period']]).min().reset_index()
df_min['period_name'] = df_min['period'].apply(lambda x: 'Period_'+str(x))
df_min['Attr'] = 'Min'
df_max = df['Result'].groupby([df['EMPI'], df['period']]).max().reset_index()
df_max['period_name'] = df_max['period'].apply(lambda x: 'Period_'+str(x))
df_max['Attr'] = 'Max'
df_sd = df['Result'].groupby([df['EMPI'], df['period']]).std().reset_index()
df_sd['period_name'] = df_sd['period'].apply(lambda x: 'Period_'+str(x))
df_sd['Attr'] = 'SD'
df_nobs = df['Result'].groupby([df['EMPI'], df['period']]).size().reset_index()
df_nobs['period_name'] = df_nobs['period'].apply(lambda x: 'Period_'+str(x))
df_nobs['Attr'] = 'Nobs'

df_concat = pd.concat([df_mean, df_sd, df_min, df_max, df_nobs])

df_concat_w=df_concat.pivot_table(index='EMPI', columns=['period_name', 'Attr'], values='Result')

cols = df_concat_w.columns
newcols = [(k+'_'+v) for k, v in cols]

df_concat_w.columns = newcols

df_concat_w.to_csv(
"/Users/yc794/Documents/ManningLab/Partners_Biobank_Longitudinal_Diabetes_Cohort/results/dd/Trajectory_datafile_of_BMI_"+str(datetime.date.today())+".csv",
index=True)
