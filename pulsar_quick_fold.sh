#!/usr/bin/bash

OBSID=${1}
PULSAR=${2}
DATADIR=${3}
#DATADIR=/scratch2/mwaops/stremblay/data/observations/${OBSID}/fits/${OBSID}_00${FITSFILES}.fits
#DATADIR=/scratch2/mwaops/vcs/1133329792/pointings/19:45:14.00_-31:47:36.00/1133329792*.fits

psrcat -e $PULSAR > ${PULSAR}.eph
EPHEM=${PULSAR}.eph
DM=`grep -A0 'DM ' $EPHEM | awk '{print $2}'`

echo $OBSID $PULSAR $DATADIR $EPHEM $DM


#PREPFOLD
prepfoldscript=prepfoldscript.batch

# Make the batch file
{ 
echo "#!/bin/bash -l"
echo
echo "#SBATCH --output=prepfold_${OBSID}_${PULSAR}.out"
echo "#SBATCH --job-name=prepfold_${OBSID}_${PULSAR}"
echo "#SBATCH --partition=workq"
echo "#SBATCH --time=02:00:00"
echo "#SBATCH --nodes=1"
echo "export OMP_NUM_THREADS=20"
echo
#echo "aprun -b -cc none -d 20 prepfold -ncpus 20 -o ${OBSID} -runavg -noclip -par ${EPHEM} -nsub 256 ${DATADIR}"
echo "aprun -b -cc none -d 20 prepfold -ncpus 20 -o ${OBSID} -runavg -noclip -par ${EPHEM} -nsub 256 ${DATADIR}"
echo "rm ${PULSAR}.eph"
} >> ${prepfoldscript}

#cat ${prepfoldscript}
sbatch ${prepfoldscript}

rm ${prepfoldscript}






