import RPDR_parsing as rp
import argparse
import pandas as pd

parser = argparse.ArgumentParser(description='A filelist of input / output files, one filename per row no quote marks')
parser.add_argument('-F', required=True, help='full path to the Enc')
parser.add_argument('-I', required=True, help='full path to the IM clinics json file')
parser.add_argument('-O1', required=True, help='full path to the IM EMPI output file')
parser.add_argument('-O2', required=True, help='full path to the IM EMPI output file')

args = parser.parse_args()
fileEnc = args.F
itemlist = args.I
fileout1 = args.O1
fileout2 = args.O2

enc = rp.RPDR_query(name="Enc", filein=fileEnc)
m = rp.matchterm(name="IM_clinics", filein=itemlist)

df = rp.read_matched(rpdrobj=enc, matchtermobj=m)

imdf = df.groupby(df.EMPI).size().reset_index()
imdf = imdf.rename(columns={0: "count"})

im2df = imdf[imdf['count'] > 1]

imdf = imdf[['EMPI']]
im2df = im2df[['EMPI']]

imdf.to_csv(fileout1, index=False, header=False)
im2df.to_csv(fileout2, index=False, header=False)

