# dealing with complex request like document 4, sql could be more flexible and accessible in data manipulation
# get matched data first
# use SQL to deal with combined data in order to derive those variables requested
# the derivation procedure will get multiple variables involved from different perspective:
# Obese, Morbid_obese: BMI and Weight(lbs)
# CKD_60, CKD_45, CKD_30, ERSD, CKD_control: Diagnosis, Diagnosis_Code, eGFR and date
# max_A1c: A1c, EMPI
# Microalb_max: malb, EMPI
# Max_HDL, Min_HDL: HDL, EMPI
# Max_trig, Min_trig: Trig, EMPI
# Statin_ever, Lipidmed_ever, Diamed_ever, Insulin_ever, Oad_ever: Med
# Statin_now, Lipidmed_now, Lipidmed_name, Diamed_now, Insulin_now, Oad_now, Oad_names: Med, Date

from functools import reduce
from datetime import datetime
from multiprocessing import Pool
import RPDR_parsing as rp
import pandas as pd
import argparse
import sqlite3
import math
import time
import re

start_time = time.time()

parser = argparse.ArgumentParser(description='A filelist of input / output files, one filename per row no quote marks')
parser.add_argument('-Dem', required=True, help='full path to the Dem')
parser.add_argument('-ID', required=True, help='full path to the EMPI list')
parser.add_argument('-P', required=True, help='full path to the Phy')
parser.add_argument('-PI1', required=True, help='full path to the weight test item list file')
parser.add_argument('-PI2', required=True, help='full path to the bmi test item list file')
parser.add_argument('-PI3', required=True, help='full path to the hba1c test item list file')
parser.add_argument('-PI4', required=True, help='full path to the creatinine test item list file')
parser.add_argument('-PI5', required=True, help='full path to the eGFR test item list file')
parser.add_argument('-PI6', required=True, help='full path to the malb test item list file')
parser.add_argument('-PI7', required=True, help='full path to the hdl test item list file')
parser.add_argument('-PI8', required=True, help='full path to the trig test item list file')
parser.add_argument('-L', required=True, help='full path to the Lab')
parser.add_argument('-LI1', required=True, help='full path to the HbA1c test item list file')
parser.add_argument('-LI2', required=True, help='full path to the creatinine test item list file')
parser.add_argument('-LI3', required=True, help='full path to the eGFR test item list file')
parser.add_argument('-LI4', required=True, help='full path to the HDL test item list file')
parser.add_argument('-LI5', required=True, help='full path to the Trig test item list file')
parser.add_argument('-M', required=True, help='full path to the Med')
parser.add_argument('-MI1', required=True, help='full path to the lipidmed item list file')
parser.add_argument('-MI2', required=True, help='full path to the Diamed item list file')
parser.add_argument('-MI3', required=True, help='full path to the Insulin item list file')
# -	Outpatient medications only (remove inpatient rows)
parser.add_argument('-MI4', required=True, help='full path to the Statin item list file')
parser.add_argument('-MI5', required=True, help='full path to the non-Insulin item list file')
parser.add_argument('-D', required=True, help='full path to the Dia')
parser.add_argument('-DI1', required=True, help='full path to the renal failure diagnosis list file')
parser.add_argument('-DI2', required=True, help='full path to the end stage renal failure diagnosis list file')
parser.add_argument('-DC1', required=True, help='full path to the renal failure diagnosis list file')
parser.add_argument('-DC2', required=True, help='full path to the end stage renal failure diagnosis list file')
parser.add_argument('-O', required=True, help='full path to the output file')

args = parser.parse_args()
fileDem = args.Dem
filePhy = args.P
fileLab = args.L
fileMed = args.M
fileDia = args.D
fileOut = args.O

dem = rp.RPDR_query(name="Dem", filein=fileDem)
phy = rp.RPDR_query(name='Phy', filein=filePhy)
lab = rp.RPDR_query(name='Lab', filein=fileLab)
med = rp.RPDR_query(name='Med', filein=fileMed)
dia = rp.RPDR_query(name='Dia', filein=fileDia)

mid = rp.matchterm(name="IDGRP", filein=args.ID)
mwtphy = rp.matchterm(name="Weight", filein=args.PI1)
mbmiphy = rp.matchterm(name="BMI", filein=args.PI2)
ma1cphy = rp.matchterm(name="A1cPhy", filein=args.PI3)
ma1clab = rp.matchterm(name="A1cLab", filein=args.LI1)
mcrephy = rp.matchterm(name="crePhy", filein=args.PI4)
mcrelab = rp.matchterm(name="creLab", filein=args.LI2)
mgfrphy = rp.matchterm(name="gfrPhy", filein=args.PI5)
mgfrlab = rp.matchterm(name="gfrLab", filein=args.LI3)
malbphy = rp.matchterm(name="ma1bphy", filein=args.PI6)
mhdlphy = rp.matchterm(name="hdlphy", filein=args.PI7)
mhdllab = rp.matchterm(name="hdllab", filein=args.LI4)
mtrigphy = rp.matchterm(name="trigPhy", filein=args.PI8)
mtriglab = rp.matchterm(name="triglab", filein=args.LI5)
mlipidsmed = rp.matchterm(name="lipidmed", filein=args.MI1)
mdmmed = rp.matchterm(name="dmmed", filein=args.MI2)
minsulinmed = rp.matchterm(name="insulin", filein=args.MI3)
mstatinmed = rp.matchterm(name="statin", filein=args.MI4)
mnoninsulinmed = rp.matchterm(name="noninsulin", filein=args.MI5)
mrenaldia1 = rp.matchterm(name="renalfailure", filein=args.DI1)
mrenaldia2 = rp.matchterm(name="endstagerenalfailure", filein=args.DI2)
mrenaldiac1 = rp.matchterm(name="renalfailureC", filein=args.DC1)
mrenaldiac2 = rp.matchterm(name="endstagerenalfailureC", filein=args.DC2)

maplist = [
            [phy, mwtphy], [phy, mbmiphy],
            [phy, ma1cphy], [lab, ma1clab],
            [phy, mcrephy], [lab, mcrelab],
            [phy, mgfrphy], [lab, mgfrlab],
            [phy, malbphy],
            [phy, mhdlphy], [lab, mhdllab],
            [phy, mtrigphy], [lab, mtriglab],
            [med, mlipidsmed], [med, mdmmed], [med, minsulinmed], [med, mstatinmed], [med, mnoninsulinmed],
            [dia, mrenaldia1], [dia, mrenaldia2], [dia, mrenaldiac1], [dia, mrenaldiac2]
]

# read in dem
# dem used for eGFR calculation
demdf = rp.read_matched(rpdrobj=dem, matchtermobj=mid)
demdf['Age'] = pd.to_numeric(demdf['Age'], errors='coerce')

#demdf = rp.read_matched(rpdrobj=dem, matchtermobj=mbib)


p = Pool()
result = p.starmap(rp.read_matched, maplist)
p.close()
p.join()

reslist = {}
for i in range(len(result)):
    if result[i].columns[-1] not in reslist.keys():
        reslist[result[i].columns[-1]] = result[i]

del result
#print(reslist)


# those dataframes need to be processed in different strategies, it is hard to manipulate them directly.
# there should be a way to modifying the result from multiprocessing in a more effective way.

for k, df in reslist.items():
    if k == mwtphy.name:
        df = df.rename(columns={"Concept_Name": "Weight"})
        df['Result'] = pd.to_numeric(df['Result'], errors='coerce')
        #print(df.columns)
        df['Result'] = df.apply(lambda row:
                                row.Result*2.2046 if row.Units.upper() in ["KILOGRAMS", "KILOGRAM"]
                                else row.Result, axis=1)
        weightmax = df.groupby(['EMPI'])['Result'].agg(['max'])
        weightmax = weightmax.add_prefix("Weight_").reset_index()
        weightmax["Weight_ge260lbs"] = weightmax["Weight_max"].apply(lambda x: 1 if x >= 260 else 0)
        weightmax["Weight_ge345lbs"] = weightmax["Weight_max"].apply(lambda x: 1 if x >= 345 else 0)

    if k == mbmiphy.name:
        df = df.rename(columns={"Concept_Name": "BMI"})
        df['Result'] = pd.to_numeric(df['Result'], errors='coerce')
        bmimax = df.groupby(['EMPI'])['Result'].agg(['max'])
        bmimax = bmimax.add_prefix("BMI_").reset_index()
        bmimax["BMI_ge30"] = bmimax["BMI_max"].apply(lambda x: 1 if x >= 30 else 0)

    if k == ma1cphy.name:
        df = df.rename(columns={"Concept_Name": "A1c"})
        df['Result'] = df['Result'].map(lambda x: re.sub(r'%', '', x))
        df['Result'] = df['Result'].map(lambda x: re.sub(r'[a-zA-Z]', '', x))
        df['Result'] = pd.to_numeric(df['Result'], errors='coerce')
        a1cphymax = df.groupby(['EMPI'])['Result'].agg(['max'])
        a1cphymax = a1cphymax.add_prefix("HbA1c_").reset_index()

    if k == ma1clab.name:
        df = df.rename(columns={"Group_Id": "A1c", "Seq_Date_Time": "Date"})
        df['Result'] = df['Result'].map(lambda x: re.sub(r'%', '', x))
        df['Result'] = df['Result'].map(lambda x: re.sub(r'[a-zA-Z]', '', x))
        df['Result'] = pd.to_numeric(df['Result'], errors='coerce')
        a1clabmax = df.groupby(['EMPI'])['Result'].agg(['max'])
        a1clabmax = a1clabmax.add_prefix("HbA1c_").reset_index()

    if k == mcrephy.name:
        df = df.rename(columns={"Concept_Name": "Creatinine"})
        df['Result'] = pd.to_numeric(df['Result'], errors='coerce')
        crephy = df.copy()

    if k == mcrelab.name:
        df = df.rename(columns={"Group_Id": "Creatinine", "Seq_Date_Time": "Date"})
        df['Result'] = pd.to_numeric(df['Result'], errors='coerce')
        crelab = df.copy()

    if k == mgfrphy.name:
        df = df.rename(columns={"Concept_Name": "eGFR"})
        df['Result'] = pd.to_numeric(df['Result'], errors='coerce')
        gfrphy = df.copy()
        gfrphy["eGFR_lt60"] = gfrphy['Result'].apply(lambda x: 1 if x < 60 else 0)
        gfrphy["eGFR_lt45"] = gfrphy['Result'].apply(lambda x: 1 if x < 45 else 0)
        gfrphy["eGFR_lt30"] = gfrphy['Result'].apply(lambda x: 1 if x < 30 else 0)
        gfrphy["eGFR_lt15"] = gfrphy['Result'].apply(lambda x: 1 if x < 15 else 0)

    if k == mgfrlab.name:
        df = df.rename(columns={"Group_Id": "eGFR", "Seq_Date_Time": "Date"})
        df['Result'] = pd.to_numeric(df['Result'], errors='coerce')
        gfrlab = df.copy()
        gfrlab["eGFR_lt60"] = gfrlab['Result'].apply(lambda x: 1 if x < 60 else 0)
        gfrlab["eGFR_lt45"] = gfrlab['Result'].apply(lambda x: 1 if x < 45 else 0)
        gfrlab["eGFR_lt30"] = gfrlab['Result'].apply(lambda x: 1 if x < 30 else 0)
        gfrlab["eGFR_lt15"] = gfrlab['Result'].apply(lambda x: 1 if x < 15 else 0)

    if k == malbphy.name:
        df = df.rename(columns={"Concept_Name": "malb"})
        df['Result'] = pd.to_numeric(df['Result'], errors='coerce')
        malbphymax = df.groupby(['EMPI'])['Result'].agg(['max'])
        malbphymax = malbphymax.add_prefix("Microalb_").reset_index()

    if k == mhdlphy.name:
        df = df.rename(columns={"Concept_Name": "HDL"})
        df['Result'] = pd.to_numeric(df['Result'], errors='coerce')
        hdlphy = df.copy()

    if k == mhdllab.name:
        df = df.rename(columns={"Group_Id": "HDL", "Seq_Date_Time": "Date"})
        df['Result'] = pd.to_numeric(df['Result'], errors='coerce')
        hdllab = df.copy()

    if k == mtrigphy.name:
        df = df.rename(columns={"Concept_Name": "Trig"})
        df['Result'] = pd.to_numeric(df['Result'], errors='coerce')
        trigphy = df.copy()

    if k == mtriglab.name:
        df = df.rename(columns={"Group_Id": "Trig", "Seq_Date_Time": "Date"})
        df['Result'] = pd.to_numeric(df['Result'], errors='coerce')
        triglab = df.copy()

    if k == mlipidsmed.name:
        df = df.rename(columns={"Medication": "Lipidmed_name"})
        df["Medication_Date"] = pd.to_datetime(df['Medication_Date'])
        lipidmedraw = df.copy()
        lipidmedraw = lipidmedraw.sort_values(['EMPI', 'Medication_Date']).drop_duplicates(subset='EMPI', keep='last')
        lipidmedraw = lipidmedraw[["EMPI", "Lipidmed_name"]]
        sdate = '3/3/2019'
        sdate = datetime.strptime(sdate, "%m/%d/%Y")
        df["Lipid_now"] = df['Medication_Date'].apply(lambda x: 1 if x >= sdate else 0)
        lipidmed = df.groupby(['EMPI'])['Lipid_now'].agg(['max'])
        lipidmed = lipidmed.add_prefix("Lipid_now_").reset_index()
        lipidmed["Lipid_ever"] = 1
        lipidmed = reduce(lambda left, right: pd.merge(left, right, on=['EMPI'], how='outer'), [lipidmed, lipidmedraw])
        del lipidmedraw

    if k == mdmmed.name:
        df = df.rename(columns={"Medication": "Diamed"})
        df["Medication_Date"] = pd.to_datetime(df['Medication_Date'])
        sdate = '3/3/2019'
        sdate = datetime.strptime(sdate, "%m/%d/%Y")
        df["Diamed_now"] = df['Medication_Date'].apply(lambda x: 1 if x >= sdate else 0)
        diamed = df.groupby(['EMPI'])['Diamed_now'].agg(['max'])
        diamed = diamed.add_prefix("Diamed_now_").reset_index()
        diamed["Diamed_ever"] = 1

    if k == minsulinmed.name:
        df = df.rename(columns={"Medication": "Insulin"})
        # limit to only outpatient insulin
        df = df.loc[df.Inpatient_Outpatient.str.upper() == "OUTPATIENT"]
        df["Medication_Date"] = pd.to_datetime(df['Medication_Date'])
        sdate = '3/3/2019'
        sdate = datetime.strptime(sdate, "%m/%d/%Y")
        df["Insulin_now"] = df['Medication_Date'].apply(lambda x: 1 if x >= sdate else 0)
        insulin = df.groupby(['EMPI'])['Insulin_now'].agg(['max'])
        insulin = insulin.add_prefix("Insulin_now_").reset_index()
        insulin["Insulin_ever"] = 1

    if k == mstatinmed.name:
        df = df.rename(columns={"Medication": "Statin"})
        df["Medication_Date"] = pd.to_datetime(df['Medication_Date'])
        sdate = '3/3/2019'
        sdate = datetime.strptime(sdate, "%m/%d/%Y")
        df["Statin_now"] = df['Medication_Date'].apply(lambda x: 1 if x >= sdate else 0)
        statin = df.groupby(['EMPI'])['Statin_now'].agg(['max'])
        statin = statin.add_prefix("Statin_now_").reset_index()
        statin["Statin_ever"] = 1

    if k == mnoninsulinmed.name:
        df = df.rename(columns={"Medication": "Oad_names"})
        df["Medication_Date"] = pd.to_datetime(df['Medication_Date'])
        oadmedraw = df.copy()
        oadmedraw = oadmedraw.sort_values(['EMPI', 'Medication_Date']).drop_duplicates(subset='EMPI', keep='last')
        oadmedraw = oadmedraw[["EMPI", "Oad_names"]]
        sdate = '3/3/2019'
        sdate = datetime.strptime(sdate, "%m/%d/%Y")
        df["Oadmed_now"] = df['Medication_Date'].apply(lambda x: 1 if x >= sdate else 0)
        oadmed = df.groupby(['EMPI'])['Oadmed_now'].agg(['max'])
        oadmed = oadmed.add_prefix("Oadmed_now_").reset_index()
        oadmed["Oadmed_ever"] = 1
        oadmed = reduce(lambda left, right: pd.merge(left, right, on=['EMPI'], how='outer'), [oadmed, oadmedraw])
        del oadmedraw

    if k == mrenaldia1.name:
        df = df.rename(columns={"Diagnosis_Name": "renalfailure"})
        df["Renal_failure"] = 1
        renalfaild = df.groupby(['EMPI'])['Renal_failure'].agg(['count']).add_prefix("renalfaild_").reset_index()

    if k == mrenaldia2.name:
        df = df.rename(columns={"Diagnosis_Name": "EndStagerenalfailure"})
        df["EndStage_Renal_failure"] = 1
        esrenalfaild = df.groupby(['EMPI'])['EndStage_Renal_failure'].agg(['count']).add_prefix("esrenalfaild_").reset_index()

    if k == mrenaldiac1.name:
        df = df.rename(columns={"Code": "renalfailureCode"})
        df["Renal_failure"] = 1
        renalfailc = df.groupby(['EMPI'])['renalfailureCode'].agg(['count']).add_prefix("renalfailc_").reset_index()

    if k == mrenaldiac2.name:
        df = df.rename(columns={"Code": "EndStagerenalfailureCode"})
        df["EndStage_Renal_failure"] = 1
        esrenalfailc = df.groupby(['EMPI'])['EndStagerenalfailureCode'].agg(['count']).add_prefix("esrenalfailc_").reset_index()

renalfail = reduce(lambda left, right: pd.merge(left, right, on=['EMPI'], how='outer'), [renalfaild, renalfailc])
esrenalfail = reduce(lambda left, right: pd.merge(left, right, on=['EMPI'], how='outer'), [esrenalfaild, esrenalfailc])

renalfail["renalfail"] = renalfail.apply(lambda row:
                                         1 if (row.renalfaild_count > 0 or row.renalfailc_count > 0)
                                         else 0, axis=1)
esrenalfail["esrenalfail"] = esrenalfail.apply(lambda row:
                                         1 if (row.esrenalfaild_count > 0 or row.esrenalfailc_count > 0)
                                         else 0, axis=1)
del renalfaild
del renalfailc
del esrenalfaild
del esrenalfailc

#ICD9 ICD10
print(reslist)
del reslist
# concatenate data from more than 1 source
a1c_df = pd.concat([a1cphymax, a1clabmax], axis=0, ignore_index=True)
cre_df = pd.concat([crephy, crelab], axis=0, ignore_index=True)
gfr_df = pd.concat([gfrphy, gfrlab], axis=0, ignore_index=True)
hdl_df = pd.concat([hdlphy, hdllab], axis=0, ignore_index=True)
trig_df = pd.concat([trigphy, triglab], axis=0, ignore_index=True)

del a1cphymax
del a1clabmax
del crephy
del crelab
del gfrphy
del gfrlab
del hdlphy
del hdllab
del trigphy
del triglab

# get maximum value for concatenated dataframes
a1cmax_df = a1c_df.groupby(['EMPI'])['HbA1c_max'].agg(['max']).add_suffix("_A1c").reset_index()
hdlstat_df = hdl_df.groupby(['EMPI'])['Result'].agg(['max', 'min']).add_suffix("_HDL").reset_index()
trigstat_df = trig_df.groupby(['EMPI'])['Result'].agg(['max', 'min']).add_suffix("_trig").reset_index()

del a1c_df
del hdl_df
del trig_df

# derive value for obesity
obese = reduce(lambda left, right: pd.merge(left, right, on=['EMPI'], how='outer'), [bmimax, weightmax])
obese["Obese"] = obese.apply(lambda row: 1 if row.Weight_ge260lbs == 1 or row.BMI_ge30 == 1 else 0, axis=1)
obese["Morbid_obese"] = obese.apply(lambda row: 1 if row.Weight_ge345lbs == 1 or row.BMI_ge30 == 1 else 0, axis=1)

# CKD related variable derivation
race = re.compile("black", re.IGNORECASE)
demdf["EMPI"] = demdf["EMPI"].astype('object')
cre_df["EMPI"] = cre_df["EMPI"].astype('object')
cre_df = reduce(lambda left, right: pd.merge(left, right, on=['EMPI'], how='right'), [demdf, cre_df])
# derived calculated eGFR based on creatinine
cre_df["cal_eGFR"] = cre_df.apply(lambda row:
                                  141*(min(row.Result/0.7, 1)**(-0.329))*(max(row.Result/0.7, 1)**(-1.209))*(0.993**row.Age)*1.018*1.159
                                  if row.Gender == "Female" and bool(race.search(row.Race))
                                  else (141*(min(row.Result/0.9, 1)**(-0.411))*(max(row.Result/0.9, 1)**(-1.209))*(0.993**row.Age)*1.159
                                        if row.Gender == "Male" and bool(race.search(row.Race))
                                        else (141*(min(row.Result/0.7, 1)**(-0.329))*(max(row.Result/0.7, 1)**(-1.209))*(0.993**row.Age)*1.018
                                              if row.Gender == "Female" and bool(race.search(row.Race))
                                              else 141*(min(row.Result/0.9, 1)**(-0.411))*(max(row.Result/0.9, 1)**(-1.209))*(0.993**row.Age))),
                                  axis=1)
print(cre_df.cal_eGFR.head())
# concatenate eGFR and creatinine
gfr_df = pd.concat([gfr_df, cre_df], axis=0, ignore_index=True)
# assign value
gfr_df["eGFR"] = gfr_df.apply(lambda row: row.Result if math.isnan(row.cal_eGFR) else row.cal_eGFR, axis=1)
gfr_df["CKD_lt60"] = gfr_df["eGFR"].apply(lambda x: 1 if x < 60 else 0)
gfr_df["CKD_lt45"] = gfr_df["eGFR"].apply(lambda x: 1 if x < 45 else 0)
gfr_df["CKD_lt30"] = gfr_df["eGFR"].apply(lambda x: 1 if x < 30 else 0)
gfr_df["CKD_lt15"] = gfr_df["eGFR"].apply(lambda x: 1 if x < 15 else 0)
gfr_df["Date"] = pd.to_datetime(gfr_df["Date"])
# CKD_control
CKD = reduce(lambda left, right: pd.merge(left, right, on=['EMPI'], how='outer'), [renalfail, gfr_df])
CKD["CKD_control"] = CKD.apply(lambda row: 1 if row.renalfail == 1 or row.CKD_lt60 == 1 else 0, axis=1)

CKD_lt60 = gfr_df.loc[gfr_df["CKD_lt60"] == 1]
CKD_60 = CKD_lt60.groupby(['EMPI'])['Date'].agg(['last', 'first']).assign(diff=lambda x: abs(x.pop('last') - x.pop('first'))).add_prefix("CKD_lt60").reset_index()
CKD_lt45 = gfr_df.loc[gfr_df["CKD_lt45"] == 1]
CKD_45 = CKD_lt45.groupby(['EMPI'])['Date'].agg(['last', 'first']).assign(diff=lambda x: abs(x.pop('last') - x.pop('first'))).add_prefix("CKD_lt45").reset_index()
CKD_lt30 = gfr_df.loc[gfr_df["CKD_lt30"] == 1]
CKD_30 = CKD_lt30.groupby(['EMPI'])['Date'].agg(['last', 'first']).assign(diff=lambda x: abs(x.pop('last') - x.pop('first'))).add_prefix("CKD_lt30").reset_index()
CKD_lt15 = gfr_df.loc[gfr_df["CKD_lt15"] == 1]
CKD_15 = CKD_lt15.groupby(['EMPI'])['Date'].agg(['last', 'first']).assign(diff=lambda x: abs(x.pop('last') - x.pop('first'))).add_prefix("CKD_lt15").reset_index()
del CKD_lt60
del CKD_lt45
del CKD_lt30
del CKD_lt15

# max time interval
CKD_60_tmax = CKD_60.groupby(["EMPI"])["CKD_lt60diff"].agg(['max']).add_prefix("CKD_lt60_tidff").reset_index()
CKD_45_tmax = CKD_45.groupby(["EMPI"])["CKD_lt45diff"].agg(['max']).add_prefix("CKD_lt45_tidff").reset_index()
CKD_30_tmax = CKD_30.groupby(["EMPI"])["CKD_lt30diff"].agg(['max']).add_prefix("CKD_lt30_tidff").reset_index()
CKD_15_tmax = CKD_15.groupby(["EMPI"])["CKD_lt15diff"].agg(['max']).add_prefix("CKD_lt15_tidff").reset_index()

del CKD_60
del CKD_45
del CKD_30
del CKD_15

# mark out max time interval >= 30
CKD_60_tmax = CKD_60_tmax.loc[CKD_60_tmax["CKD_lt60_tidffmax"].dt.days >= 30]
CKD_60_tmax["CKD_lt60d"] = 1

CKD_45_tmax = CKD_45_tmax.loc[CKD_45_tmax["CKD_lt45_tidffmax"].dt.days >= 30]
CKD_45_tmax["CKD_lt45d"] = 1

CKD_30_tmax = CKD_30_tmax.loc[CKD_30_tmax["CKD_lt30_tidffmax"].dt.days >= 30]
CKD_30_tmax["CKD_lt30d"] = 1

CKD_15_tmax = CKD_15_tmax.loc[CKD_15_tmax["CKD_lt15_tidffmax"].dt.days >= 30]
CKD_15_tmax["CKD_lt15d"] = 1

CKD_60_c = reduce(lambda left, right: pd.merge(left, right, on=['EMPI'], how='outer'), [renalfail, CKD_60_tmax])
#print(CKD_60_c.head())
CKD_60_c["CKD_60"] = CKD_60_c.apply(lambda row: 1 if (row.renalfail == 1 or row.CKD_lt60d == 1) else 0, axis=1)
CKD_45_c = reduce(lambda left, right: pd.merge(left, right, on=['EMPI'], how='outer'), [esrenalfail, CKD_45_tmax])
CKD_45_c["CKD_45"] = CKD_45_c.apply(lambda row: 1 if (row.esrenalfail == 1 or row.CKD_lt45d == 1) else 0, axis=1)
CKD_30_c = reduce(lambda left, right: pd.merge(left, right, on=['EMPI'], how='outer'), [esrenalfail, CKD_30_tmax])
CKD_30_c["CKD_30"] = CKD_30_c.apply(lambda row: 1 if (row.esrenalfail == 1 or row.CKD_lt30d == 1) else 0, axis=1)
CKD_15_c = reduce(lambda left, right: pd.merge(left, right, on=['EMPI'], how='outer'), [esrenalfail, CKD_15_tmax])
CKD_15_c["ESRD"] = CKD_15_c.apply(lambda row: 1 if (row.esrenalfail == 1 or row.CKD_lt15d == 1) else 0, axis=1)

del CKD_60_tmax
del CKD_45_tmax
del CKD_30_tmax
del CKD_15_tmax
del renalfail
del esrenalfail

comboset = [demdf, obese,
            CKD, CKD_60_c, CKD_45_c, CKD_30_c, CKD_15_c, a1cmax_df,
            malbphymax, hdlstat_df, trigstat_df,
            statin, lipidmed, diamed, insulin, oadmed]

combo = reduce(lambda left, right: pd.merge(left, right, on=['EMPI'], how='outer'), comboset)

outcols = ["EMPI", "Obese", "Morbid_obese",
           "CKD_60", "CKD_45", "CKD_30", "ESRD", "CKD_control",
           "max_A1c", "Microalb_max",
           "max_HDL", "min_HDL", "max_trig", "min_trig",
           "Statin_ever", "Statin_now_max", "Lipid_ever", "Lipid_now_max", "Lipidmed_name",
           "Diamed_ever", "Diamed_now_max",
           "Insulin_ever", "Insulin_now_max", "Oadmed_ever", "Oadmed_now_max", "Oad_names"]

combo = combo[outcols]

combo.to_csv(fileOut, header = False, index = False)
print("--- %s seconds ---" % (time.time() - start_time))
