base_path = "/Users/yc794/Documents/ManningLab/Partners_Biobank_Longitudinal_Diabetes_Cohort/a_dat/clinical_variables/"
hba1c_path = "HbA1c_08JAN2020.csv"
gluc_path = "Gluc_08JAN2020.csv"

import pandas as pd
import numpy as np

df = pd.read_csv(base_path+gluc_path)
df.columns = df.columns.str.strip()
df['EMPI'] = df.EMPI.apply(str)
df = df[df.Dt_yr >= 2000]
df.drop_duplicates(inplace=True)
df.dropna(subset=['ResultVal'])
df['abs_dt_diff'] = abs(df.Dt_diff)
#dt_diff = df[['EMPI', 'abs_dt_diff', 'Dt_yr']]
df['min_abs_dt_diff'] = df.EMPI.map(df.groupby('EMPI')['abs_dt_diff'].min())
df = df[df.min_abs_dt_diff == df.abs_dt_diff]
df['min_dt_yr'] = df.EMPI.map(df.groupby('EMPI')['Dt_yr'].min())
df = df[df.Dt_yr == df.min_dt_yr]
df = df[['EMPI', 'Dt_yr', 'abs_dt_diff', 'min_abs_dt_diff', 'min_dt_yr']]
df.drop_duplicates(inplace=True)

out = df[['EMPI', 'Dt_yr']]

out.to_csv('test_gluc_bsl.csv', index=False)
#%timeit df2['Data3'].groupby(df['Date']).transform('sum')
#%timeit df2.groupby('Date')['Data3'].transform('sum')
#df.Date.map(df.groupby('Date')['Data3'].sum())

#dt_diff.EMPI.map(dt_diff.groupby('EMPI')[['abs_dt_diff','Dt_yr']].sum())
# min_dt_diff = min_dt_diff[['EMPI','Dt_yr']]
# min_dt_yr = min_dt_diff.groupby(['EMPI']).min().reset_index()

# def RecAround03(datfl):
#    df = pd.read_csv(datfl)
#    df = df[df.Dt_yr > 2000]
