# This file could be used to get the test item information out from Phy and Lab, as a potential reference for keyword search
# Dia and Med files could also output Diagnosis information(Non-code) and Medication_Date_Detail
import RPDR_parsing as rp
import argparse

parser = argparse.ArgumentParser(description='A filelist of RPDR filelist, one filename per row no quote marks')
parser.add_argument('-F', required=True, help='full path to the filelist.txt')
parser.add_argument('-O', required=True, help='full path to the output item data file')
parser.add_argument('-N', required=True, help='Name of the RPDR file')
args = parser.parse_args()
filein = args.F
fileout = args.O
name = args.N

query = rp.RPDR_query(name=name, filein=filein)
outdat, header = query.read_data()

item = set(line[5] for line in outdat)

with open(fileout, 'w') as f:
    for i in item:
        f.write(i+'\n')
f.close()

