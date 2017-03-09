#!/bin/sh

#PULSAR=${1}
#OBSID=${2}
#ACCELCAND=${3}
#DATAFILE="$OBSID"/fits/*.fits  #Only needs the MJD part. eg 1095506112/fits/1095506112_000*
#NAME=test_"$PULSAR"_"$OBSID" #DON'T TO FORGET TO PUT _ INFRONT. Comment name to help me identify it
NAME=${1}
:'
psrcat -e $PULSAR > "$PULSAR".eph
DM=`grep -A0 'DM ' "$PULSAR".eph | awk '{print $2}'`
PERIOD=0.3877


realfftsccript=realfftsccript.batch

# Make the batch file
echo "#!/bin/bash -l" > $realfftsccript
echo >> $realfftsccript
echo "#SBATCH --output=fft_"${NAME: -7}".out" >> $realfftsccript
echo "#SBATCH --job-name=fft_"${NAME: -7}"" >> $realfftsccript
echo "#SBATCH --partition=workq" >> $realfftsccript
echo "#SBATCH --time=0:10:00" >> $realfftsccript
echo "#SBATCH --nodes=1" >> $realfftsccript
echo "export OMP_NUM_THREADS=20" >> $realfftsccript
echo "aprun -b -cc none -d 20 realfft "$NAME".dat" >> $realfftsccript

# ...and run it
jobid=`sbatch $realfftsccript| cut -d " " -f 4`

rm $realfftsccript
'

accelscript=accelscript.batch

# Make the batch file
echo "#!/bin/bash -l" > $accelscript
echo >> $accelscript
echo "#SBATCH --output=accel_"${NAME: -7}".out" >> $accelscript
echo "#SBATCH --job-name=accel_"${NAME: -7}"" >> $accelscript
echo "#SBATCH --partition=workq" >> $accelscript
echo "#SBATCH --time=11:59:00" >> $accelscript
#echo "#SBATCH --nodes=5" >> $accelscript
#echo "#SBATCH --dependency=afterany:"${jobid}
echo "export OMP_NUM_THREADS=20" >> $accelscript
echo "aprun -b -cc none  -d 20 accelsearch -ncpus 20 -flo 0.1 -fhi 100 -numharm 8 "$NAME".fft" >> $accelscript


# ...and run it
sbatch $accelscript

rm $accelscript

#use data
#echo "aprun -b -cc none -d 20 prepfold -ncpus 20 -n 128 -nsub 128 -runavg -accelcand $ACCELCAND -accelfile "$NAME"_ACCEL_200.cand  -o "$NAME"_withdat -topo "$NAME".dat" >> $realfftsccript
#no search
#echo "aprun -b -cc none -d 20 prepfold -ncpus 20 -n 128 -nsub 128 -runavg -accelcand $ACCELCAND -accelfile "$NAME"_ACCEL_200.cand -dm $DM -p $PERIOD -o "$NAME"_fits_nosearch -topo -nosearch /scratch2/mwaops/stremblay/data/observations/"$DATAFILE"" >> $realfftsccript
#use fits
#echo "aprun -b -cc none -d 20 prepfold -ncpus 20 -n 128 -nsub 128 -runavg -accelcand $ACCELCAND -accelfile "$NAME"_ACCEL_200.cand  -o "$NAME"_fits -topo /scratch2/mwaops/stremblay/data/observations/"$DATAFILE"" >> $realfftsccript  
#echo "aprun -b -cc none -d 20 accelsearch -h" >> $realfftsccript 

#1133329792 J1900-2600
#1120799848 J0835-4510
#           J0837-4135
#1116090392 J1645-0317
#           J1709-1640
#           J1820-0427 #faint scattered
#
