import RPDR_parsing as rp
import argparse
import pandas as pd
from functools import reduce


parser = argparse.ArgumentParser(description='A filelist of input / output files, one filename per row no quote marks')
parser.add_argument('-B', required=True, help='full path to the Bib')
parser.add_argument('-C', required=True, help='full path to the Con')
parser.add_argument('-E', required=True, help='full path to the Enc')
parser.add_argument('-O', required=True, help='full path to the output file')

args = parser.parse_args()
fileBib = args.B
fileCon = args.C
fileEnc = args.E
fileout = args.O

bib = rp.RPDR_query(name='Bib', filein=fileBib)
con = rp.RPDR_query(name='Con', filein=fileCon)
enc = rp.RPDR_query(name='Enc', filein=fileEnc)

bibdf = bib.lst_2_pd()
condf = con.lst_2_pd()
encdf = enc.lst_2_pd()

encdf['last_visit_date'] = pd.to_datetime(encdf.Admit_Date)
# if there might be potential missingness in the data file, run below, might be slow for big data files:
# encdf['last_visit_date'] = encdf['Admit_Date'].apply(lambda x: pd.to_datetime(x) if(pd.notnull(x)) else x)
# if the date format in all RPDR files is in ISO 8601, another code could be used, might be faster:
# encdf['last_visit_date'] = pd.to_datetime(encdf.Admit_Date, format = '%d/%m/%y')


encdf = encdf.sort_values(['EMPI', 'last_visit_date']).drop_duplicates(subset='EMPI', keep='last')

cols = {'Bib': ['EMPI'],
        'Con': ['EMPI', 'Address1', 'Address2', 'City', 'State', 'Zip', 'Country'],
        'Enc': ['EMPI', 'last_visit_date']}

bibdf = bibdf[cols['Bib']]
condf = condf[cols['Con']]
encdf = encdf[cols['Enc']]

combo = [bibdf, condf, encdf]
df = reduce(lambda left, right: pd.merge(left, right, on=['EMPI'], how='outer'), combo)

df.to_csv(fileout, index=False)