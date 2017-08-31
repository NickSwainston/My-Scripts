#!/bin/sh

########################
## using “convert” from ImageMagick to do ps convert into PNG
#########################

input=${1}
for psname in `ls $input | grep .ps `
do
newname=`basename $psname .ps`
echo "PS convert to PNG, please wait the process"
convert -density 150 -geometry 100% $psname -rotate 90 "$newname".png
done
chmod -R 6774 /scratch2/mwaops/pulsar/incoh_census/1146512968
