# filelist
base_pth = "/Users/yc794/Documents/ManningLab/Partners_Biobank_Longitudinal_Diabetes_Cohort/a_dat/"
incident_fl = "Cohort_building/Magda/Incident_glucose_IDlist_requested_by_Magda_2020-04-22.csv"
case_b4_bsl_fl = "Cohort_building/Magda/Case_before_clinical_baseline_glucose_IDlist_requested_by_Magda_2020-04-22.csv"
prevalent_fl = "Cohort_building/Magda/Prevalent_IDlist_requested_by_Magda_2020-04-22.csv"
suspicious_unk_fl = "Cohort_building/T2D_Case_selection_result_suspicious_unknown_2020-03-04.csv"

survey_var_pthlst = "biobank_survey/filelist"
demo_var_pthlst = "demographic_variables/filelist"
clinical_var_pthlst = "clinical_variables/filelist"
last_records_pthlst = "Cohort_building/last_clinical_records/filelist"

import pandas as pd
import numpy as np
import math

# read in datafiles
header_list = ["path"]
survey_var_fllst = pd.read_csv(base_pth + survey_var_pthlst, names=header_list)
demo_var_fllst = pd.read_csv(base_pth + demo_var_pthlst, names=header_list)
clinical_var_fllst = pd.read_csv(base_pth + clinical_var_pthlst, names=header_list)
last_records_fllst = pd.read_csv(base_pth + last_records_pthlst, names=header_list)

demo_var_fllst = demo_var_fllst[demo_var_fllst.path.str.contains('Dob_gluc_bsl') |
                                demo_var_fllst.path.str.contains('Dod_gluc_bsl')]
last_records_fllst = last_records_fllst[last_records_fllst.path.str.contains('LastClinical_gluc_bsl')]

survey_var_fllst['flname'] = survey_var_fllst.path.apply(lambda x: x.split('/')[-1])
demo_var_fllst['flname'] = demo_var_fllst.path.apply(lambda x: x.split('/')[-1])
clinical_var_fllst['flname'] = clinical_var_fllst.path.apply(lambda x: x.split('/')[-1])
last_records_fllst['flname'] = last_records_fllst.path.apply(lambda x: x.split('/')[-1])

survey_var_fllst['label'] = survey_var_fllst.flname.apply(lambda x: x.split('_')[0])
demo_var_fllst['label'] = demo_var_fllst.flname.apply(lambda x: x.split('_')[0])
clinical_var_fllst['label'] = clinical_var_fllst.flname.apply(lambda x: x.split('_')[0])
last_records_fllst['label'] = last_records_fllst.flname.apply(lambda x: x.split('_')[0])

print(survey_var_fllst.head())
print(demo_var_fllst.head())
print(clinical_var_fllst.head())
print(last_records_fllst.head())

for row in clinical_var_fllst.itertuples():
    clinical_var_fl = pd.read_csv(row.path, low_memory=False)
    clinical_var_fl = clinical_var_fl[clinical_var_fl.Dt_yr >= 2000]
    clinical_var_fl['period'] = clinical_var_fl.Dt_yr.apply(lambda x: math.ceil((x-2000)/2))
    clinical_var_fl['period'] = np.where((clinical_var_fl.Dt_yr == 2000), 1, clinical_var_fl.period)
    clinical_var_fl.columns = clinical_var_fl.columns.str.strip()
    clinical_var_fl = clinical_var_fl[['EMPI', 'period', 'ResultVal']]

    clinical_mean = clinical_var_fl.groupby(['EMPI', 'period']).mean().reset_index()
    clinical_mean['EMPI'] = str(clinical_mean.EMPI)
    clinical_mean['period'] = int(clinical_mean.period)

    clinical_sd = clinical_var_fl.groupby(['EMPI', 'period']).std().reset_index()
    clinical_sd['EMPI'] = str(clinical_sd.EMPI)
    clinical_sd['period'] = int(clinical_sd.period)

    clinical_max = clinical_var_fl.groupby(['EMPI', 'period']).max().reset_index()
    clinical_max['EMPI'] = str(clinical_max.EMPI)
    clinical_max['period'] = int(clinical_max.period)

    clinical_median = clinical_var_fl.groupby(['EMPI', 'period']).median().reset_index()
    clinical_median['EMPI'] = str(clinical_median.EMPI)
    clinical_median['period'] = int(clinical_median.period)

    clinical_nobs = clinical_var_fl.groupby(['EMPI', 'period']).size().reset_index()
    clinical_nobs['EMPI'] = str(clinical_nobs.EMPI)
    clinical_nobs['period'] = int(clinical_nobs.period)

    df.rename(columns={"A": "a", "B": "c"})
    df1.merge(df2, left_on='lkey', right_on='rkey')
# gapminder[['continent','lifeExp']].groupby('continent').mean()