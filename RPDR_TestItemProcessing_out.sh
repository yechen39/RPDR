BASEDIR=/Users/yc794/Documents/ManningLab/Partners_Biobank_Longitudinal_Diabetes_Cohort
INDIR=/Volumes/LaCie/PBB
OUTDIR=$BASEDIR/scripts/Data_files_creating
NAME=$1
SCRIPT=$BASEDIR/scripts/Data_files_creating/Python

mkdir $BASEDIR/a_dat/RPDR_test/
find $INDIR | grep $NAME | sort > $BASEDIR/a_dat/RPDR_test/filelist_$NAME

python3 $SCRIPT/RPDR_TestItemProcessing_out.py \
-F $BASEDIR/a_dat/RPDR_test/filelist_$NAME \
-O $OUTDIR/Item_$1.txt \
-N $NAME 
