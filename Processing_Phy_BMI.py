import RPDR_parsing as rp
import pandas as pd
import timeit

filein = '/Volumes/LaCie/PBB/Phy_filelist'

phy = rp.RPDR_query(name='Phy', filein=filein)
phydat, phyheader = phy.read_data()

item = set(line[5] for line in phydat)

#phydf = pd.DataFrame(phydat, columns=phyheader)
