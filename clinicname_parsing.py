import re
import argparse

#parser = argparse.ArgumentParser(description='A file of clinic names, one clinic name/code per row no quote marks')
#parser.add_argument('-F', required=True, help='full path to the clinic list file.txt')
#parser.add_argument('-O', required=True, help='full path to the output data file')
#args = parser.parse_args()
filein = "/Users/yc794/Documents/ManningLab/Partners_Biobank_Longitudinal_Diabetes_Cohort/a_dat/IM_list.txt"
fileout = "/Users/yc794/Documents/ManningLab/Partners_Biobank_Longitudinal_Diabetes_Cohort/a_dat/test_IM_list.txt"

with open(filein, "r") as f:
    codes = []
    names = []
    lines = []
    for line in f:
        parse_line = line.strip()
        lines.append(parse_line)
        if bool(re.findall(r'\(.*?\)', line)):
            code = re.findall(r'\(.*?\)', line)[0]
            codes.append(code)
            name, _, a = line.partition('(')
            name = name.strip()
            names.append(name)
    f.close()

out = lines + names + codes

#with open(fileout, 'w') as f:
#    for i in out:
#        f.write(i + '\n')
#f.close()