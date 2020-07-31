import RPDR_parsing as rp
import pandas as pd
import argparse
from functools import reduce
import re
import math

parser = argparse.ArgumentParser(description='A filelist of input / output files, one filename per row no quote marks')
parser.add_argument('-P', required=True, help='full path to the Phy')
parser.add_argument('-PI', required=True, help='full path to the Phy HbA1c test item list file')
parser.add_argument('-L', required=True, help='full path to the Lab')
parser.add_argument('-LI', required=True, help='full path to the Lab HbA1c test item list file')
parser.add_argument('-M', required=True, help='full path to the Med')
parser.add_argument('-MI1', required=True, help='full path to the T1D Med')
parser.add_argument('-MI2', required=True, help='full path to the T2D Med')
parser.add_argument('-D', required=True, help='full path to the Dia')
parser.add_argument('-DI1', required=True, help='full path to the T1D Dia')
parser.add_argument('-DI2', required=True, help='full path to the T2D Dia')
parser.add_argument('-B', required=True, help='full path to the Bib')
parser.add_argument('-BI', required=True, help='full path to the IM EMPI match file')
parser.add_argument('-O', required=True, help='full path to the BMI output file')

args = parser.parse_args()
fileBib = args.B
filePhy = args.P
fileLab = args.L
fileMed = args.M
fileDia = args.D
fileout = args.O

cols = {'Bib': ['EMPI', 'Subject_Id'],
        'Lab': ['EMPI', 'Date', 'Group_Id', 'Result', 'Units', 'Reference_Range', 'DataSource'],
        'Phy': ['EMPI', 'Date', 'Concept_Name', 'Result', 'Units', 'Clinic', 'Inpatient_Outpatient', 'DataSource'],
        'Med': ['EMPI', 'Medication_Date', 'Medication'],
        'Dia': ['EMPI', 'Date', 'Diagnosis_Name', 'Code_Type', 'Code']
        }

bib = rp.RPDR_query(name="Bib", filein=fileBib)
phy = rp.RPDR_query(name="Phy", filein=filePhy)
lab = rp.RPDR_query(name="Lab", filein=fileLab)
med = rp.RPDR_query(name="Med", filein=fileMed)
dia = rp.RPDR_query(name="Dia", filein=fileDia)

mbib = rp.matchterm(name="IM", filein=args.BI)
ma1c_phy = rp.matchterm(name="A1c_Phy", filein=args.PI)
ma1c_lab = rp.matchterm(name="A1c_Lab", filein=args.LI)
mmed1 = rp.matchterm(name="T1DRx", filein=args.MI1)
mmed2 = rp.matchterm(name="T2DRx", filein=args.MI2)
mdia1 = rp.matchterm(name="T1DDx", filein=args.DI1)
mdia2 = rp.matchterm(name="T2DDx", filein=args.DI2)

# subset EMPI for subjects with IM visits based on decision tree
bibdf = rp.read_matched(rpdrobj=bib, matchtermobj=mbib)
bibdf = bibdf[cols['Bib']]
print(bibdf.head())
print(bibdf.shape)
# read in HbA1c records, both in Lab and Phy
a1cphydf = rp.read_matched(rpdrobj=phy, matchtermobj=ma1c_phy)
a1clabdf = rp.read_matched(rpdrobj=lab, matchtermobj=ma1c_lab)
print(a1cphydf.head())
print(a1cphydf.shape)
print(a1clabdf.head())
print(a1clabdf.shape)
# label DataSource
a1cphydf['DataSource'] = 'Phy'
a1clabdf['DataSource'] = 'Lab'
# rename columns for further concatenation
a1clabdf = a1clabdf.rename(columns={"Reference_Units": "Units", "Seq_Date_Time": "Date"})
a1cphydf = a1cphydf[cols['Phy']]
a1clabdf = a1clabdf[cols['Lab']]
a1cdf = pd.concat([a1cphydf, a1clabdf], axis=0, ignore_index=True)
# clear unused variables
del a1cphydf
del a1clabdf
# derive year value based on date
a1cdf['Date'] = pd.to_datetime(a1cdf['Date'])
a1cdf['year'] = pd.DatetimeIndex(a1cdf['Date']).year
# derive period value based on year
a1cdf['period'] = a1cdf['year'].apply(lambda x: ((x-2000+1)//2) if x > 2002 else 1)
# get max, mean, sd, count of HbA1c for every subject per period
a1cdf['Result'] = a1cdf['Result'].map(lambda x: re.sub(r'\%', '', x))
a1cdf['Result'] = a1cdf['Result'].map(lambda x: re.sub(r'[a-zA-Z]', '', x))
a1cdf['Result'] = pd.to_numeric(a1cdf['Result'], errors='coerce')
a1csum = a1cdf.groupby(['EMPI', 'period'])['Result'].agg(['max', 'mean', 'std', 'count'])
a1csum = a1csum.add_prefix("HbA1c_").reset_index()
a1csum['hba1c_flg'] = a1csum['HbA1c_max'].apply(lambda x: 2 if x >= 6.5 else (1 if 5.7 <= x < 6.5 else (-9 if math.isnan(x) else 0)))
print(a1csum)

# read in medication records
med1df = rp.read_matched(rpdrobj=med, matchtermobj=mmed1)
med2df = rp.read_matched(rpdrobj=med, matchtermobj=mmed2)
# remove aspartame and emergency
med1df = med1df[med1df.Clinic.str.lower() != "emergency"]
tmp = re.compile("aspartame", re.IGNORECASE)
med1df['tmp'] = med1df['Medication'].apply(lambda x: bool(tmp.search(x)))
med1df = med1df[med1df.tmp == False]
med1df = med1df[cols['Med']]
med2df = med2df[cols['Med']]
# rename columns for further merge
med1df = med1df.rename(columns={"Medication_Date": "T1DRxDt", "Medication": "T1DRx"})
med2df = med2df.rename(columns={"Medication_Date": "T2DRxDt", "Medication": "T2DRx"})
print(med1df.head())
print(med1df.shape)
print(med2df.head())
print(med2df.shape)
# derive year value based prescription date
med1df['T1DRxDt'] = pd.to_datetime(med1df['T1DRxDt'])
med2df['T2DRxDt'] = pd.to_datetime(med2df['T2DRxDt'])
med1df['year'] = pd.DatetimeIndex(med1df['T1DRxDt']).year
med2df['year'] = pd.DatetimeIndex(med2df['T2DRxDt']).year
# derive period value based on year
med1df['period'] = med1df['year'].apply(lambda x: ((x-2000+1)//2) if x > 2002 else 1)
med2df['period'] = med2df['year'].apply(lambda x: ((x-2000+1)//2) if x > 2002 else 1)
# count the # of prescription for every subject per period
med1sum = med1df.groupby(['EMPI', 'period'])['T1DRxDt'].agg(['count'])
med1sum = med1sum.add_prefix("T1DRx_").reset_index()
med2sum = med2df.groupby(['EMPI', 'period'])['T2DRxDt'].agg(['count'])
med2sum = med2sum.add_prefix("T2DRx_").reset_index()
# get the earliest T1DRx and T2DRx
med1min = med1df.groupby(['EMPI'])['T1DRxDt'].agg(['min'])
med1min = med1min.add_prefix("T1DRxDt_").reset_index()
med2min = med2df.groupby(['EMPI'])['T2DRxDt'].agg(['min'])
med2min = med2min.add_prefix("T2DRxDt_").reset_index()
# merge
meddf = reduce(lambda left, right: pd.merge(left, right, on=['EMPI', 'period'], how='outer'), [med1sum, med2sum])
meddf = reduce(lambda left, right: pd.merge(left, right, on=['EMPI'], how='outer'), [meddf, med1min, med2min])
# med1min, med2min
# derive Dxout based on the earliest DxDt
meddf['Rxout'] = meddf.apply(lambda row: row['T1DRxDt_min'] < row['T2DRxDt_min'], axis=1)
# clear unused variables
del med1df
del med2df
del med1sum
del med2sum
del med1min
del med2min

# read in diagnosis records
dia1df = rp.read_matched(rpdrobj=dia, matchtermobj=mdia1)
dia2df = rp.read_matched(rpdrobj=dia, matchtermobj=mdia2)
dia1df = dia1df[cols['Dia']]
dia2df = dia2df[cols['Dia']]
# rename columns for further merge
dia1df = dia1df.rename(columns={"Date": "T1DDxDt", "Diagnosis_Name": "T1DDx", 'Code': 'T1DDxCD'})
dia2df = dia2df.rename(columns={"Date": "T2DDxDt", "Diagnosis_Name": "T2DDx", 'Code': 'T2DDxCD'})
# derive year value based prescription date
dia1df['T1DDxDt'] = pd.to_datetime(dia1df['T1DDxDt'])
dia2df['T2DDxDt'] = pd.to_datetime(dia2df['T2DDxDt'])
dia1df['year'] = pd.DatetimeIndex(dia1df['T1DDxDt']).year
dia2df['year'] = pd.DatetimeIndex(dia2df['T2DDxDt']).year
# derive period value based on year
dia1df['period'] = dia1df['year'].apply(lambda x: ((x-2000+1)//2) if x > 2002 else 1)
print(dia1df.head())
print(dia1df.shape)
dia2df['period'] = dia2df['year'].apply(lambda x: ((x-2000+1)//2) if x > 2002 else 1)
print(dia2df.head())
print(dia2df.shape)
# derive PhyscnDia, PhyscnDia_sum, physcn_flg value based on diagnosis Code_Type
dia2df['PhyscnDia'] = dia2df.Code_Type.apply(lambda x: 1 if x.upper in ['LMR', 'ONCALL'] else 0)
dia2physcn = dia2df.groupby(['EMPI', 'period'])['PhyscnDia'].agg(['sum'])
dia2physcn = dia2physcn.add_prefix('PhyscnDia_').reset_index()
print(dia2physcn.head())
print(dia2physcn.shape)
dia2physcn['physcn_flg'] = dia2physcn['PhyscnDia_sum'].apply(lambda x: x >= 2)
# count the # of diagnosis for every subject per period
dia1sum = dia1df.groupby(['EMPI', 'period'])['T1DDxDt'].agg(['count'])
dia1sum = dia1sum.add_prefix('T1DDx_').reset_index()
dia2sum = dia2df.groupby(['EMPI', 'period'])['T2DDxDt'].agg(['count'])
dia2sum = dia2sum.add_prefix('T2DDx_').reset_index()
# get the earliest T1DDx and T2DDx
dia1min = dia1df.groupby(['EMPI'])['T1DDxDt'].agg(['min'])
dia1min = dia1min.add_prefix('T1DDxDt_').reset_index()
dia2min = dia2df.groupby(['EMPI'])['T2DDxDt'].agg(['min'])
dia2min = dia2min.add_prefix('T2DDxDt_').reset_index()
# merge
diadf = reduce(lambda left, right: pd.merge(left, right, on=['EMPI', 'period'], how='outer'), [dia1sum, dia2sum, dia2physcn])
diadf = reduce(lambda left, right: pd.merge(left, right, on=['EMPI'], how='outer'), [diadf, dia1min, dia2min])
# derive Dxout based on the earliest DxDt
diadf['Dxout'] = diadf.apply(lambda row: row['T1DDxDt_min'] < row['T2DDxDt_min'], axis=1)
# clear unused variables
del dia1df
del dia2df
del dia1sum
del dia2sum
del dia1min
del dia2min
del dia2physcn

# merge all datafiles
combo = reduce(lambda left, right: pd.merge(left, right, on=['EMPI', 'period'], how='outer'), [a1csum, meddf, diadf])
combo = reduce(lambda left, right: pd.merge(left, right, on=['EMPI'], how='outer'), [bibdf, combo])
del a1csum
del meddf
del diadf
del bibdf

# logical conditions for case selections
conditions = ['Dxout', 'Rxout', 'hba1c_flg',
              'T1DDx_count', 'T2DDx_count', 'T1DRx_count', 'T2DRx_count', 'physcn_flg']

combo['T1DDx_count'] = combo['T1DDx_count'].apply(lambda x: 0 if math.isnan(x) else x)
combo['T2DDx_count'] = combo['T2DDx_count'].apply(lambda x: 0 if math.isnan(x) else x)
combo['T1DRx_count'] = combo['T1DRx_count'].apply(lambda x: 0 if math.isnan(x) else x)
combo['T2DRx_count'] = combo['T2DRx_count'].apply(lambda x: 0 if math.isnan(x) else x)
combo['physcn_flg'] = combo['physcn_flg'].apply(lambda x: 0 if math.isnan(x) else x)
combo['hba1c_flg'] = combo['hba1c_flg'].apply(lambda x: -9 if math.isnan(x) else x)

# if not combo.Dxout and combo.T2DDx_count > 0 and combo.T1DRx_count > 0
# and combo.T2DRx_count > 0 and not combo.Rxout
# if not combo.Dxout and combo.T2DDx_count > 0 and combo.T1DRx_count > 0
# and combo.T2DRx_count == 0 and combo.physcn_flg
# if not combo.Dxout and combo.T2DDx_count > 0 and combo.T1DRx_count == 0
# and combo.T2DRx_count == 0 and combo.hba1c_flg == 2
# if not combo.Dxout and combo.T2DDx_count == 0 and combo.T2DRx_count > 0
# and combo.hba1c_flg == 2

# prediabetes
# if not combo.Dxout and combo.T2DDx_count == 0 and combo.T1DRx_count == 0
# and combo.T2DRx_count == 0 and combo.hba1c_flg == 1

caseselection = {
        2: "T2D",
        1: "Prediabetes",
        0: "Unknown"
}
print(caseselection)
combo["T2Dcase"] = combo.apply(lambda row :
                               2 if (not row.Dxout and row.T2DDx_count > 0 and row.T1DRx_count > 0 and row.T2DRx_count > 0 and not row.Rxout)
                               or (not row.Dxout and row.T2DDx_count > 0 and row.T1DRx_count > 0 and row.T2DRx_count == 0 and row.physcn_flg)
                               or (not row.Dxout and row.T2DDx_count > 0 and row.T1DRx_count == 0 and row.T2DRx_count == 0 and row.hba1c_flg == 2)
                               or (not row.Dxout and row.T2DDx_count == 0 and row.T2DRx_count > 0 and row.hba1c_flg == 2)
                               else (1 if (not row.Dxout and row.T2DDx_count == 0 and row.T1DRx_count == 0 and row.T2DRx_count == 0 and row.hba1c_flg == 1)
                                     else 0), axis=1)

susunk = {
        1: "Suspicious Unknown due to abnormal HbA1c value",
        2: "Suspicious Unknown due to abnormal nonzero T1D Diagnosis",
        3: "Suspicious Unknown due to abnormal nonzero T2D Diagnosis",
        4: "Suspicious Unknown due to abnormal nonzero T1D Medication",
        5: "Suspicious Unknown due to abnormal nonzero T2D Medication",
        6: "Suspicious Unknown due to abnormal T1D Diagnosis before T2D Diagnosis",
        7: "Suspicious Unknown due to abnormal T1D Medication before T2D Medication",
        8: "To be decided"
}
print(susunk)

combo['suspicious_unknown'] = combo.apply(lambda row:
                                          1 if row.T2Dcase == 0 and row.hba1c_flg > 0
                                          else (2 if row.T2Dcase == 0 and row.T1DDx_count > 0
                                                else (3 if row.T2Dcase == 0 and row.T2DDx_count > 0
                                                      else (4 if row.T2Dcase == 0 and row.T1DRx_count > 0
                                                            else (5 if row.T2Dcase == 0 and row.T2DRx_count
                                                                  else (6 if row.T2Dcase == 0 and row.Dxout
                                                                        else(7 if row.T2Dcase == 0 and row.Rxout
                                                                             else 8)))))), axis=1)

combo = combo.dropna(axis=0, subset=['period'])
combo['T2Dcase'] = combo.apply(lambda row:
                               1.5 if (row['T2Dcase'] == 0 and row['suspicious_unknown'] < 8)
                               else row['T2Dcase'], axis=1)

print(combo.head())
print(combo.shape)
print("Case selection results")
print(combo.T2Dcase.value_counts())
print("Suspicious Unknown")
print(combo.suspicious_unknown.value_counts())
print("Case selection results by period")
print(combo.groupby(['period'], as_index=False)['T2Dcase'].count())
print("Suspicious Unknown by period")
print(combo.groupby(['period'], as_index=False)['suspicious_unknown'].count())

# inherit value from previous periods
combo_wide = combo.pivot(index='EMPI', columns='period', values='T2Dcase')

combo_wide['r_period_1'] = combo_wide[1]
combo_wide['r_period_2'] = combo_wide.apply(lambda row:
                                            row[2] if math.isnan(row['r_period_1'])
                                            else (row['r_period_1'] if math.isnan(row[2])
                                                  else max(row['r_period_1'], row[2])), axis=1)
combo_wide['r_period_3'] = combo_wide.apply(lambda row:
                                            row[3] if math.isnan(row['r_period_2'])
                                            else (row['r_period_2'] if math.isnan(row[3])
                                                  else max(row['r_period_2'], row[3])), axis=1)
combo_wide['r_period_4'] = combo_wide.apply(lambda row:
                                            row[4] if math.isnan(row['r_period_3'])
                                            else (row['r_period_3'] if math.isnan(row[4])
                                                  else max(row['r_period_3'], row[4])), axis=1)
combo_wide['r_period_5'] = combo_wide.apply(lambda row:
                                            row[5] if math.isnan(row['r_period_4'])
                                            else (row['r_period_4'] if math.isnan(row[5])
                                                  else max(row['r_period_4'], row[5])), axis=1)
combo_wide['r_period_6'] = combo_wide.apply(lambda row:
                                            row[6] if math.isnan(row['r_period_5'])
                                            else (row['r_period_5'] if math.isnan(row[6])
                                                  else max(row['r_period_5'], row[6])), axis=1)
combo_wide['r_period_7'] = combo_wide.apply(lambda row:
                                            row[7] if math.isnan(row['r_period_6'])
                                            else (row['r_period_6'] if math.isnan(row[7])
                                                  else max(row['r_period_6'], row[7])), axis=1)
combo_wide['r_period_8'] = combo_wide.apply(lambda row:
                                            row[8] if math.isnan(row['r_period_7'])
                                            else (row['r_period_7'] if math.isnan(row[8])
                                                  else max(row['r_period_7'], row[8])), axis=1)
combo_wide['r_period_9'] = combo_wide.apply(lambda row:
                                            row[9] if math.isnan(row['r_period_8'])
                                            else (row['r_period_8'] if math.isnan(row[9])
                                                  else max(row['r_period_8'], row[9])), axis=1)
combo_wide['r_period_10'] = combo_wide.apply(lambda row:
                                            row[10] if math.isnan(row['r_period_9'])
                                            else (row['r_period_9'] if math.isnan(row[10])
                                                  else max(row['r_period_9'], row[10])), axis=1)
combo_wide['EMPI'] = combo_wide.index

combo_w2l = pd.melt(combo_wide, id_vars='EMPI',
                    value_vars=['r_period_1', 'r_period_2', 'r_period_3', 'r_period_4', 'r_period_5',
                                'r_period_6', 'r_period_7', 'r_period_8', 'r_period_9', 'r_period_10'],
                    var_name='rperiod',
                    value_name='rT2Dcase')
combo_w2l['period'] = combo_w2l['rperiod'].str.extract('(\d+)')
combo_w2l["period"] = pd.to_numeric(combo_w2l["period"])

combo_all = reduce(lambda left, right: pd.merge(left, right, on=['EMPI', 'period'], how='outer'), [combo, combo_w2l])
del combo
del combo_w2l

combo_all = combo_all.sort_values(['EMPI', 'period']).reset_index()
combo_all = combo_all.drop(['index', 'rperiod'], axis=1)

print(combo_all.head())
print(combo_all.shape)
print("Case selection results")
print(combo_all.rT2Dcase.value_counts())
print("Case selection results by period")
print(combo_all.groupby(['period'], as_index=False)['rT2Dcase'].count())

combo_all.to_csv(fileout, index=False)