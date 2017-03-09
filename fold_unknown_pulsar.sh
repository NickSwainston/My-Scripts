#!/usr/bin/bash

OBSID=${1}
DM=83.028
PERIOD=0.009552694
PERIODD=0.0000000006
#FITSFILES=${2}
#DATADIR=/scratch2/mwaops/vcs/${OBSID}/fits/*.fits
DATADIR=/scratch2/mwaops/vcs/1146512968/pointings/16:11:41.08_-29:57:18.72/114*
#DATADIR=/scratch2/mwaops/vcs/1133329792/pointings/19:45:14.00_-31:47:36.00/1133329792*.fits
:'
psrcat -e $PULSAR > ${PULSAR}.eph
EPHEM=${PULSAR}.eph
DM=`grep -A0 "DM " $EPHEM | awk "{print $2}"`


#RFIFIND
rfifindscript=rfifindscript.batch

# Make the batch file
{  
echo "#!/bin/bash -l"
echo ""
echo "#SBATCH --output=${OBSID}_rfifindscript.out"
echo "#SBATCH --job-name=rfifind_${OBSID}"
echo "#SBATCH --partition=workq"
echo "#SBATCH --time=02:00:00"
echo "#SBATCH --nodes=1"
echo "export OMP_NUM_THREADS=20"
echo ""
echo "aprun -b -cc none -d 20 rfifind -ncpus 20 -noclip -time 12.0 -o ${OBSID} -zapchan `/group/mwaops/bmeyers/zapchan.py -rfifind` ${DATADIR}" 
} >> ${rfifindscript}

#cat ${rfifindscript}
sbatch ${rfifindscript} >> JOBNUM.temp
chmod 775 JOBNUM.temp

array=($(cat JOBNUM.temp))
i=${array[3]}
rm JOBNUM.temp

rm ${rfifindscript}

echo $OBSID  $DATADIR $DM
'
#PREPFOLD
prepfoldscript=prepfoldscript.batch

# Make the batch file
{ 
echo "#!/bin/bash -l"
echo
echo "#SBATCH --output=prepfoldscript.out"
echo "#SBATCH --job-name=prepfold_${OBSID}_${PULSAR}"
echo "#SBATCH --partition=workq"
echo "#SBATCH --time=02:00:00"
echo "#SBATCH --nodes=1"
#echo "#SBATCH --dependency=afterany:"${i}
echo "export OMP_NUM_THREADS=20"
echo
echo "aprun -b -cc none -d 20 prepfold -ncpus 20 -o ${OBSID} -topo -runavg -noclip -nsub 256 -p $PERIOD -pd $PERIODD -dm $DM -mask ${OBSID}_rfifind.mask ${DATADIR}"
} >> ${prepfoldscript}

#cat ${prepfoldscript}
sbatch ${prepfoldscript}

rm ${prepfoldscript}
