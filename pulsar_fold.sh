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


#RFIFIND
rfifindscript=rfifindscript.batch

rfichans="0:19,108:127,128:147,236:255,256:275,364:383,384:403,492:511,512:531,620:639,640:659,748:767,768:787,876:895,896:915,1004:1023,1024:1043,1132:1151,1152:1171,1260:1279,1280:1299,1388:1407,1408:1427,1516:1535,1536:1555,1644:1663,1664:1683,1772:1791,1792:1811,1900:1919,1920:1939,2028:2047,2048:2067,2156:2175,2176:2195,2284:2303,2304:2323,2412:2431,2432:2451,2540:2559,2560:2579,2668:2687,2688:2707,2796:2815,2816:2835,2924:2943,2944:2963,3052:3071"


# Make the batch file
{  
echo "#!/bin/bash -l"
echo ""
echo "#SBATCH --output=${OBSID}_rfifindscript.out"
echo "#SBATCH --job-name=rfifind_${OBSID}_${PULSAR}"
echo "#SBATCH --partition=workq"
echo "#SBATCH --time=03:00:00"
echo "#SBATCH --nodes=1"
echo "export OMP_NUM_THREADS=20"
echo ""
echo "aprun -b -cc none -d 20 rfifind -ncpus 20 -noclip -time 12.0 -o ${OBSID} -zapchan ${rfichans}  ${DATADIR}" 
} >> ${rfifindscript}

cat ${rfifindscript}
sbatch ${rfifindscript} >> JOBNUM.temp
chmod 775 JOBNUM.temp

array=($(cat JOBNUM.temp))
i=${array[3]}
rm JOBNUM.temp

rm ${rfifindscript}




#PREPDATA
prepdatascript=prepdatascript.batch

# Make the batch file
{
echo "#!/bin/bash -l"
echo
echo "#SBATCH --output=${OBSID}_prepdatascript.out"
echo "#SBATCH --job-name=prepdata_${OBSID}_${PULSAR}"
echo "#SBATCH --partition=workq"
echo "#SBATCH --time=02:00:00"
echo "#SBATCH --nodes=1"
#echo "#SBATCH --dependency=afterany:"${i}
echo "export OMP_NUM_THREADS=20"
echo
echo "aprun -b -cc none -d 20 prepdata -ncpus 20 -o zeroDM_${OBSID} -nobary -noclip  -mask ${OBSID}_rfifind.mask ${DATADIR}"
} >> ${prepdatascript}

#cat ${prepdatascript}
#sbatch ${prepdatascript} 

rm ${prepdatascript}


#PREPFOLD
prepfoldscript=prepfoldscript.batch

# Make the batch file
{ 
echo "#!/bin/bash -l"
echo
echo "#SBATCH --output=prepfoldscript.out"
echo "#SBATCH --job-name=prepfold_${OBSID}_${PULSAR}"
echo "#SBATCH --partition=workq"
echo "#SBATCH --time=11:59:00"
echo "#SBATCH --nodes=1"
echo "#SBATCH --dependency=afterany:"${i}
echo "export OMP_NUM_THREADS=20"
echo
echo "aprun -b -cc none -d 20 prepfold -ncpus 20 -o ${OBSID} -n 256 -runavg -noclip -par ${EPHEM} -mask ${OBSID}_rfifind.mask -nsub 256 ${DATADIR}"
} >> ${prepfoldscript}

#cat ${prepfoldscript}
sbatch ${prepfoldscript}

rm ${prepfoldscript}






