import argparse
import pandas as pd
import re
import sqlite3

class RPDR_query():
    instance_count = 0

    def __init__(self, name, filein):
        self.name = name
        self.filein = filein
        self.instance_count += 1

    def get_attr(self):
        print(' NAME :- {} \n SOURCE FILE :- {} \n INSTANCE COUNT :- {}'.format(self.name, self.filein, self.instance_count))

    def parse_filelist(self):
        filelist = []
        with open(self.filein, 'r') as f:
            for line in f:
                parse_line = line.strip()
                filelist.append(parse_line)
        f.close()
        return filelist

    def read_data(self):
        filelist = self.parse_filelist()
        n = 0
        data = []
        header = []
        MRN_len = 0
        IDlist = set()
        for file in filelist:
            with open(file, 'r') as f:
                for line in f:
                    if n == 0:
                        parse_line = line.strip().split('|')
                        header = parse_line
                    n += 1
                    if 'EMPI|' not in line:
                        parse_line = line.strip().split('|')
                        data.append(parse_line)
                        IDlist.add(parse_line[0])
                        MRN_len = max(MRN_len, len(parse_line[3].split(',')))
            f.close()
        print('File {} includes {} subjects with {} rows'.format(self.name, len(IDlist), n-len(filelist)))
        if n-len(filelist) > len(IDlist):
            print("Multiple entries per subject!")
        else:
            print("Single entry per subject!")
        if MRN_len > 1:
            print("Multiple MRNs per obeservation!")
        else:
            print("Single MRN per obeservation!")

        df = pd.DataFrame(data, columns=header)

        return df

    def sqlout(self):
        df = self.read_data
        conn = sqlite3.connect('')
        df.to_sql(self.name, conn, if_exist='replace', index=False)


def parsing_keywords(fileitem):
    keywords = []
    with open(fileitem, 'r') as f:
        for line in f:
            parse_line = line.strip()
            keywords.append(parse_line)
    f.close()
    return keywords

def parse_patterns(fileitem):
    with open(fileitem, 'r') as f:
        patterns = '\\b|\\b'.join(line.strip() for line in f)
        patterns = '\\b' + patterns + '\\b'
    f.close()
    return patterns

def read_matched(data=None, matchtype="exact", byvar=None, flagname = None, reffl=None, outcols=[],
                 timevar=None, sdate=None, edate=None,
                 outtype="pd", sqltblname=None, subset=True):
    # data : pandas dataframe
    # matchtype : string(possible value ["exact", "pattern"])
    # byvar : string(name for the variable to be matched upon)
    # reffl : filepath(to the file with all the keywords/patterns to be matched upon)
    # outcols : list(with names for the variables for customized output, if empty, all variables will be in ouput)
    # timevar : string(name for the time variable to be matched upon, optional)
    # edate : string(end date in "%Y-%M-%D", optional)
    # sdata : string(start date in "%Y-%M-%D", optional)
    # if timevar is given but edate and sdata are not given, the data will only be sorted by ID and time
    # outtype: string(possible value ["pd", "sql"])
    # sqltblname: string(name for output sql table, necessary of outtype is "sql")
    # subset: boolean(whether the final output will be a subset or a full set based on rows)

    # errors for necessary input
    datacopy = data.copy()
    # optional time matching
    if timevar:
        datacopy[str(timevar)] = pd.to_datetime(datacopy[str(timevar)])
        datacopy = datacopy.sort_values(['EMPI', str(timevar)])
        if not edate and sdate:
            datacopy = datacopy.loc[(datacopy[str(timevar)] >= sdate)]
        if edate and not sdate:
            datacopy = datacopy.loc[(datacopy[str(timevar)] <= edate)]
        if edate and sdate:
            datacopy = datacopy.loc[(datacopy[str(timevar)] >= sdate) & (datacopy[str(timevar)] <= edate)]
    # matching
    byflag = str(flagname) + "_flag"
    print(byflag)
    # match based on exact keywords
    if matchtype == "exact":
        ref = parsing_keywords(reffl)
        datacopy[str(byflag)] = datacopy[str(byvar)].apply(lambda x: x in ref)
    if matchtype == "pattern":
        ref = parse_patterns(reffl)
        rref = re.compile(ref, re.IGNORECASE)
        datacopy[str(byflag)] = datacopy[str(byvar)].apply(lambda x: bool(rref.search(x)))
    # check subset/fullset
    if subset:
        datacopy = datacopy.loc[datacopy[str(byflag)]]
    # columns specified for output
    if outcols:
        outcols.append(str(byflag))
        datacopy = datacopy[[outcols]]

    datacopy.reset_index()
    # output format
    if outtype == "pd":
        return datacopy
    if outtype == "sql":
        conn = sqlite3.connect('')
        datacopy.to_sql(sqltblname, conn, if_exist='replace', index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scripts for RPDR file pre-processing')
    parser.add_argument('-F', required=True, help='full path to the RPDR filelist')
    parser.add_argument('-N', required=True, help='Name of the RPDR file to be pre-processed')
    args = parser.parse_args()
    filein = args.F
    name = args.N
    datfl = RPDR_query(name=name, filein=filein)
    print(datfl.get_attr())
