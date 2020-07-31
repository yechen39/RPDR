# extracting HbA1c, random glucose lab values and body mass index values from the RPDR data set
# from 1/1/2018 – 12/31/2018 for the EMPI IDs in the attached file (EMPIs_List.csv)
# Each lab value and BMI value should have a date / encounter
# associated with it in the resulting data table

import pandas as pd
import RPDR_parsing as rp
import argparse
from functools import reduce

parser = argparse.ArgumentParser(description='A filelist of input / output files, one filename per row no quote marks')
parser.add_argument('-P', required=True, help='full path to the Phy')
parser.add_argument('-PI1', required=True, help='full path to the Phy BMI json file')
parser.add_argument('-PI2', required=True, help='full path to the Phy HbA1c json file')
parser.add_argument('-PI3', required=True, help='full path to the Phy glucose json file')
parser.add_argument('-L', required=True, help='full path to the Lab')
parser.add_argument('-LI1', required=True, help='full path to the Lab HbA1c json file')
parser.add_argument('-LI2', required=True, help='full path to the Lab glucose json file')
parser.add_argument('-B', required=True, help='full path to the Bib')
parser.add_argument('-S', required=True, help='full path to the idlist json file')
parser.add_argument('-O1', required=True, help='full path to the BMI output file')
parser.add_argument('-O2', required=True, help='full path to the A1C output file')
parser.add_argument('-O3', required=True, help='full path to the Glucose output file')


args = parser.parse_args()
fileBib = args.B
filePhy = args.P
fileLab = args.L
fileout1 = args.O1
fileout2 = args.O2
fileout3 = args.O3
idlist = args.S

bib = rp.RPDR_query(name='Bib', filein=fileBib)
phy = rp.RPDR_query(name='Phy', filein=filePhy)
lab = rp.RPDR_query(name='Lab', filein=fileLab)

mbib = rp.matchterm(name="IM", filein=idlist)
mbmi_phy = rp.matchterm(name='BMI_Phy', filein=args.PI1)
ma1c_phy = rp.matchterm(name='A1c_Phy', filein=args.PI2)
mglu_phy = rp.matchterm(name='Glu_Phy', filein=args.PI3)
ma1c_lab = rp.matchterm(name='A1c_Lab', filein=args.LI1)
mglu_lab = rp.matchterm(name='Glu_Lab', filein=args.LI2)

start_date = '1/1/2018'
end_date = '12/31/2018'

bibdf = rp.read_matched(rpdrobj=bib, matchtermobj=mbib)
bmi_phy_df = rp.read_matched(rpdrobj=phy, matchtermobj=mbmi_phy, timevar="Date", sdate=start_date, edate=end_date)
a1c_phy_df = rp.read_matched(rpdrobj=phy, matchtermobj=ma1c_phy, timevar="Date", sdate=start_date, edate=end_date)
glu_phy_df = rp.read_matched(rpdrobj=phy, matchtermobj=mglu_phy, timevar="Date", sdate=start_date, edate=end_date)
a1c_lab_df = rp.read_matched(rpdrobj=lab, matchtermobj=ma1c_lab, timevar="Seq_Date_Time", sdate=start_date, edate=end_date)
glu_lab_df = rp.read_matched(rpdrobj=lab, matchtermobj=mglu_lab, timevar="Seq_Date_Time", sdate=start_date, edate=end_date)

cols = {'Bib': ['EMPI', 'Subject_Id'],
        'Lab': ['EMPI', 'Date', 'Group_Id', 'Test_Id', 'Test_Description', 'Result', 'Result_Text',
                'Abnormal_Flag', 'Units', 'Reference_Range', 'Specimen_Type', 'Correction_Flag', 'Test_Status',
                'DataSource'],
        'Phy': ['EMPI', 'Date', 'Concept_Name', 'Code_Type', 'Result', 'Units', 'Clinic', 'Inpatient_Outpatient',
                'Encounter_number', 'DataSource']}

a1c_lab_df = a1c_lab_df.rename(columns={"Reference_Units": "Units", "Seq_Date_Time": "Date"})
glu_lab_df = glu_lab_df.rename(columns={"Reference_Units": "Units", "Seq_Date_Time": "Date"})

bmi_phy_df['DataSource'] = 'Phy'
a1c_phy_df['DataSource'] = 'Phy'
glu_phy_df['DataSource'] = 'Phy'
a1c_lab_df['DataSource'] = 'Lab'
glu_lab_df['DataSource'] = 'Lab'


bibdf = bibdf[cols['Bib']]

bmi_phy_df = bmi_phy_df[cols['Phy']]
a1c_phy_df = a1c_phy_df[cols['Phy']]
glu_phy_df = glu_phy_df[cols['Phy']]
a1c_lab_df = a1c_lab_df[cols['Lab']]
glu_lab_df = glu_lab_df[cols['Lab']]

a1c_df = pd.concat([a1c_phy_df, a1c_lab_df], axis=0, ignore_index=True)
glu_df = pd.concat([glu_phy_df, glu_lab_df], axis=0, ignore_index=True)

bmi_combo = [bibdf, bmi_phy_df]
a1c_combo = [bibdf, a1c_df]
glu_combo = [bibdf, glu_df]

bmi_combo_df = reduce(lambda left, right: pd.merge(left, right, on=['EMPI'], how='left'), bmi_combo)
a1c_combo_df = reduce(lambda left, right: pd.merge(left, right, on=['EMPI'], how='left'), a1c_combo)
glu_combo_df = reduce(lambda left, right: pd.merge(left, right, on=['EMPI'], how='left'), glu_combo)

phycolnms = ['EMPI', 'Subject_Id', 'Date', 'Concept_Name', 'Code_Type', 'Result', 'Units', 'Clinic',
             'Inpatient_Outpatient', 'Encounter_number', 'DataSource']
combocolnms = ['EMPI', 'Subject_Id', 'Date', 'Concept_Name', 'Code_Type', 'Group_Id', 'Test_Id', 'Test_Description',
               'Test_Status', 'Abnormal_Flag', 'Specimen_Type', 'Correction_Flag', 'Clinic', 'Result', 'Units',
               'Result_Text', 'Reference_Range', 'Encounter_number',  'Inpatient_Outpatient', 'DataSource']

bmi_combo_df = bmi_combo_df[phycolnms]
a1c_combo_df = a1c_combo_df[combocolnms]
glu_combo_df = glu_combo_df[combocolnms]

bmi_combo_df.to_csv(fileout1, index=False)
a1c_combo_df.to_csv(fileout2, index=False)
glu_combo_df.to_csv(fileout3, index=False)