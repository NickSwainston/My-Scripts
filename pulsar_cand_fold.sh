#!/usr/bin/bash

OBSID=${1}
POINT=${2}
CAND=${3}
CANDFILE=${4}
#DATADIR=/scratch2/mwaops/vcs/${OBSID}/fits/${OBSID}_00${FITSFILES}.fits
#DATADIR=/scratch2/mwaops/stremblay/data/observations/${OBSID}/fits/${OBSID}_00${FITSFILES}.fits
DATADIR=/scratch2/mwaops/vcs/${OBSID}/pointings/${POINT}/${OBSID}*.fits



#PREPDATA
prepdatascript=prepdatascript.batch

# Make the batch file
{
echo "#!/bin/bash -l"
echo
echo "#SBATCH --output=${OBSID}_${CAND}_prepfoldscript.out"
echo "#SBATCH --job-name=prepfold_${OBSID}_${CAND}"
echo "#SBATCH --partition=gpuq"
echo "#SBATCH --time=02:00:00"
echo "#SBATCH --nodes=1"
echo "export OMP_NUM_THREADS=8"
echo
echo "aprun -b -cc none -d 8 prepfold -ncpus 8 -o ${OBSID}_${POINT}_${CAND} -noclip -n 64 -nsub 256 -accelcand ${CAND} -accelfile ${CANDFILE} ${DATADIR}"
} > ${prepdatascript}

#cat ${prepdatascript}
sbatch ${prepdatascript} 

#rm ${prepdatascript}
