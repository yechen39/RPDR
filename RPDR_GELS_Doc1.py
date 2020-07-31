import RPDR_parsing as rp
import argparse
import pandas as pd
from functools import reduce

#/data/dgag/projects/GELS/data/RPDR_filelist/filelist_Bib
parser = argparse.ArgumentParser(description='A filelist of input / output files, one filename per row no quote marks')
parser.add_argument('-B', required=True, help='full path to the Bib')
parser.add_argument('-C', required=True, help='full path to the Con')
parser.add_argument('-D', required=True, help='full path to the Dem')
parser.add_argument('-O', required=True, help='full path to the output file')

args = parser.parse_args()
fileBib = args.B
fileCon = args.C
fileDem = args.D
fileout = args.O

bib = rp.RPDR_query(name='Bib', filein=fileBib)
con = rp.RPDR_query(name='Con', filein=fileCon)
dem = rp.RPDR_query(name='Dem', filein=fileDem)

bibdf = bib.lst_2_pd()
condf = con.lst_2_pd()
demdf = dem.lst_2_pd()

cols = {'Bib':['Subject_Id','EMPI','MGH_MRN'],
        'Con':['EMPI','Insurance_1','Insurance_2','Insurance_3'],
        'Dem':['EMPI','Gender','Date_of_Birth','Language','Race','Marital_status',
               'Religion','Is_a_veteran','Vital_status','Date_Of_Death']}

bibdf = bibdf[cols['Bib']]
condf = condf[cols['Con']]
demdf = demdf[cols['Dem']]

combo = [bibdf, condf, demdf]
df = reduce(lambda left, right: pd.merge(left, right, on=['EMPI'], how='outer'), combo)

df.to_csv(fileout, index=False)