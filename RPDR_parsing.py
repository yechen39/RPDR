import argparse
import pandas as pd
import re
import sqlite3
import json
from functools import reduce
import operator


class RPDR_query():
    instance_count = 0

    def __init__(self, name, filein):
        self.name = name
        self.filein = filein
        self.instance_count += 1
        self.get_attr()

    def get_attr(self):
        filelist = self.parse_filelist()
        with open(filelist[0], 'r') as f:
            header = f.readline()
            header = header.strip().split('|')
        print('NAME :- {} \n SOURCE FILE :- {} \n COLUMNS INCLUDED :- {} \n INSTANCE COUNT :- {}'.format(self.name, self.filein, header, self.instance_count))

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
        return header, data

class matchterm():
    instance_count = 0

    def __init__(self, name, filein):
        self.name = name
        self.filein = filein
        self.instance_count += 1
        self.get_attr()

    def get_attr(self):
        with open(self.filein, 'r') as f:
            data = json.load(f)
        jstr = json.dumps(data, indent=4)
        print('NAME :- {} \n SOURCE FILE :- {} \n MATCHTERM :- {} \n INSTANCE COUNT :- {}'.format(self.name, self.filein, jstr, self.instance_count))

    def parse_matchterm(self):
        with open(self.filein, 'r') as f:
            data = json.load(f)
        for key, value in data.items():
            if value["Method"] == "exact":
                value["KP"] = parse_keywords(value["Path"])
            if value["Method"] == "pattern":
                value["KP"] = parse_patterns(value["Path"])
        return data

def parse_keywords(fileitem):
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
        rpatterns = re.compile(patterns, re.IGNORECASE)
    f.close()
    return rpatterns

def kp_match(item, match, method):
    if method == "exact":
        return item in match
    if method == "pattern":
        return bool(match.search(item))

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except ValueError as e:
        print(e)
    finally:
        if conn:
            conn.close()

def sqlout(name=None, df=None, db_file=''):
    create_connection(db_file)
    conn = sqlite3.connect(db_file)
    df.to_sql(name, conn, if_exist='replace', index=False)

def testitemout(rpdrobj=None, testitem=None):
    filelist = rpdrobj.parse_filelist()
    with open(filelist[0], 'r') as f:
        header = f.readline()
        header = header.strip().split('|')
    f.close()
    testloc = [i for i, j in enumerate(header) if j == testitem][0]
    item = set()
    for file in filelist:
        with open(file, 'r') as f:
            for line in f:
                if 'EMPI|' not in line:
                    parse_line = line.strip().split('|')
                    item.add(parse_line[testloc])
        f.close()
    return item


def read_matched(rpdrobj=None, matchtermobj=None, outtype="pd",
                 timevar=None, sdate=None, edate=None,
                 outcols=[], logic="AND",
                 sqltblname=None, connection=None):
    # data : pandas dataframe
    # matchtype : string(possible value ["exact", "pattern"])
    # byvar : string(name for the variable to be matched upon)
    # reffl : filepath(to the file with all the keywords/patterns to be matched upon)
    # outcols : list(with names for the variables for customized output, if empty, all variables will be in ouput)
    # timevar : string(name for the time variable to be matched upon, optional)
    # edate : string(end date in "%M-%D-%D"/"%", optional)
    # sdata : string(start date in "%Y-%M-%D", optional)
    # if timevar is given but edate and sdata are not given, the data will only be sorted by ID and time
    # outtype: string(possible value ["pd", "sql"])
    # sqltblname: string(name for output sql table, necessary of outtype is "sql")
    # subset: boolean(whether the final output will be a subset or a full set based on rows)

    filelist = rpdrobj.parse_filelist()
    matchterm = matchtermobj.parse_matchterm()
    n = 0
    data = []
    header = []
    mloc = []
    MRN_len = 0
    IDlist = set()
    for file in filelist:
        with open(file, 'r') as f:
            for line in f:
                if n == 0:
                    parse_line = line.strip().split('|')
                    header = parse_line
                    mloc = [i for i, j in enumerate(header) if j in matchterm.keys()]
                    mvar = [j for i, j in enumerate(header) if j in matchterm.keys()]
                    kp = [matchterm[i]["KP"] for i in mvar]
                    method = [matchterm[i]["Method"] for i in mvar]
                n += 1
                if 'EMPI|' not in line:
                    parse_line = line.strip().split('|')
                    match = list(map(kp_match, list(map(parse_line.__getitem__, mloc)), kp, method))
                    if reduce(operator.and_, match) and logic=="AND":
                        data.append(parse_line)
                        IDlist.add(parse_line[0])
                        MRN_len = max(MRN_len, len(parse_line[3].split(',')))
                    if reduce(operator.or_, match) and logic=="OR":
                        data.append(parse_line)
                        IDlist.add(parse_line[0])
                        MRN_len = max(MRN_len, len(parse_line[3].split(',')))
        f.close()
    print('File {} includes {} subjects with {} rows'.format(str(rpdrobj), len(IDlist), n - len(filelist)))


    # output format
    df = pd.DataFrame(data=data,columns=header)
    if timevar:
        df[str(timevar)] = pd.to_datetime(df[str(timevar)])
        if not edate and sdate:
            df = df.loc[df[str(timevar)] >= sdate]
        if edate and not sdate:
            df = df.loc[df[str(timevar)] >= edate]
        if edate and sdate:
            df = df.loc[(df[str(timevar)] >= sdate) & (df[str(timevar)] <= edate)]

    if outcols:
        df = df[outcols]

    if outtype == "pd":
        return df
    if outtype == "sql":
        sqlout(name=sqltblname, df=df, db_file=connection)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scripts for RPDR file pre-processing')
    parser.add_argument('-F', required=True, help='full path to the RPDR filelist')
    parser.add_argument('-N', required=True, help='Name of the RPDR file to be pre-processed')
    args = parser.parse_args()
    filein = args.F
    name = args.N
    datfl = RPDR_query(name=name, filein=filein)
