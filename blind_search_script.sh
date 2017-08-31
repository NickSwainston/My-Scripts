#!/bin/sh

PULSAR=blind_search
DATAFILE=/scratch2/mwaops/smcsweeney/1139239952/fits/1139239952_*.fits  #Only needs the MJD part. eg 1095506112/fits/1095506112_000*
NAME=_attempt1  #DON'T TO FORGET TO PUT _ INFRONT. Comment name to help me identify it
ZERODM= #-zerodm
#^remove the comment to make it run with zerodm

:'
#time series
timeseriesscript=timeseriesscript.batch

# Make the batch file
echo "#!/bin/bash -l" > $timeseriesscript
echo >> $timeseriesscript
echo "#SBATCH --output="$PULSAR""$NAME"timeseries.out" >> $timeseriesscript
echo "#SBATCH --job-name=timeseries"$PULSAR""$NAME"" >> $timeseriesscript
echo "#SBATCH --partition=gpuq" >> $timeseriesscript
echo "#SBATCH --time=01:30:00" >> $timeseriesscript
echo "#SBATCH --nodes=1" >> $timeseriesscript
echo >> $timeseriesscript
echo "aprun -B prepdata -nobary -o zeroDM_"$PULSAR""$NAME" -mask "$PULSAR""$NAME"_rfifind.mask  $DATAFILE" >> $timeseriesscript

# ...and run it
sbatch $timeseriesscript

rm $timeseriesscript



#RFIfind

rfifindscript=rfifindscript.batch

# Make the batch file
echo "#!/bin/bash -l" > $rfifindscript
echo >> $rfifindscript
echo "#SBATCH --output="$PULSAR""$NAME"rfifind.out" >> $rfifindscript
echo "#SBATCH --job-name=rfifind"$PULSAR""$NAME"" >> $rfifindscript
echo "#SBATCH --partition=gpuq" >> $rfifindscript
echo "#SBATCH --time=01:30:00" >> $rfifindscript
echo "#SBATCH --nodes=1" >> $rfifindscript
echo >> $rfifindscript
echo "aprun -B rfifind -time 1.0 -o "$PULSAR""$NAME" -zapchan 0:19,108:127,128:147,236:255,256:275,364:383,384:403,492:511,512:531,620:639,640:659,748:767,768:787,876:895,896:915,1004:1023,1024:1043,1132:1151,1152:1171,1260:1279,1280:1299,1388:1407,1408:1427,1516:1535,1536:1555,1644:1663,1664:1683,1772:1791,1792:1811,1900:1919,1920:1939,2028:2047,2048:2067,2156:2175,2176:2195,2284:2303,2304:2323,2412:2431,2432:2451,2540:2559,2560:2579,2668:2687,2688:2707,2796:2815,2816:2835,2924:2943,2944:2963,3052:3071  $DATAFILE" >> $rfifindscript




# ...and run it
sbatch $rfifindscript >> JOBNUM.temp
chmod 775 JOBNUM.temp

array=($(cat JOBNUM.temp))
i=${array[3]}
rm JOBNUM.temp

rm $rfifindscript


#prepdata
prepdatascript=prepdatascript.batch

# Make the batch file
echo "#!/bin/bash -l" > $prepdatascript
echo >> $prepdatascript
echo "#SBATCH --output="$PULSAR""$NAME"prepdata.out" >> $prepdatascript
echo "#SBATCH --job-name=prepdata"$PULSAR""$NAME"" >> $prepdatascript
echo "#SBATCH --partition=gpuq" >> $prepdatascript
echo "#SBATCH --time=02:30:00" >> $prepdatascript
echo "#SBATCH --nodes=1" >> $prepdatascript
#echo "#SBATCH --dependency=afterany:"$i >> $prepdatascript
echo >> $prepdatascript
echo "aprun -B prepdata -mask "$PULSAR""$NAME"_rfifind.mask -o "$PULSAR""$NAME" $DATAFILE" >> $prepdatascript 

# ...and run it
sbatch $prepdatascript >> JOBNUM.temp
chmod 775 JOBNUM.temp

array=($(cat JOBNUM.temp))
i=${array[3]}
rm JOBNUM.temp

rm $prepdatascript
'
#accelsearch
accelsearchscript=accelsearchscript.batch

for datfile in $@; do
    # Make the batch file
    echo "#!/bin/bash -l" > $accelsearchscript
    echo >> $accelsearchscript
    echo "#SBATCH --output="$PULSAR""$NAME"accelsearch.out" >> $accelsearchscript
    echo "#SBATCH --job-name=accelsearch"$PULSAR""$NAME"" >> $accelsearchscript
    echo "#SBATCH --partition=gpuq" >> $accelsearchscript
    echo "#SBATCH --time=02:30:00" >> $accelsearchscript
    echo "#SBATCH --nodes=1" >> $accelsearchscript
    #echo "#SBATCH --dependency=afterany:"$i >> $accelsearchscript
    echo >> $accelsearchscript
    echo "aprun -B accelsearch  $datfile" >> $accelsearchscript 

    # ...and run it
    sbatch $accelsearchscript 
done

rm $accelsearchscript






