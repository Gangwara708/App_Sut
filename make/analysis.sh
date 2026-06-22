FILE=$1
FILE1=${FILE}_fep
FILE2=${FILE}_bep
FILE3=${FILE}_tep
mkdir ${FILE}_data
cp /home/vinay/SETUP/PERFORMANCE/BINARY_PKG/TEP/SUT/Application/make/REPORTS ${FILE}_data/
>/home/vinay/SETUP/PERFORMANCE/BINARY_PKG/TEP/SUT/Application/make/REPORTS
grep "ss7p" $FILE |awk '{ print $9 }' >${FILE}_data/$FILE1
grep "app_map " $FILE |awk '{ print $9 }' >${FILE}_data/$FILE2
grep "MAP_Appl " $FILE |awk '{ print $9 }' >${FILE}_data/$FILE3
exit
