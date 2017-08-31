#! /usr/bin/env python

"""
Author:            Mengyao XUE
Last modiication:    09/2016
-----------------------------------
This code is used to automatically fold each known pulsar covered in the PSRFITS file of the given observation ID

Thanks to Nicholas Swainston's contribution

"""
import os
import subprocess
import sqlite3
import argparse
import pdb          #pdb.set_trace() UPDATE obs_tbl set status=fold_N where obs_id=1133775752

parser = argparse.ArgumentParser(description="""
""")
parser.add_argument('-o','--obid',type=str,help="Input Observation ID")
parser.add_argument('--FITS_dir',type=str,help="Location of FITS files on system. Default /group/mwaops/ICS_data/", default = '/group/mwaops/vcs/')
parser.add_argument('--WORK_dir',type=str,help="Setting the work directory of output result. Default /scratch2/mwaops/pulsar/incoh_census/", default = '/scratch2/mwaops/pulsar/incoh_census')
parser.add_argument('-t','--rfitime',type=str,help="When calling PRESTO rfifind function, seconds to integrate for stats and FFT calcs, between 0 and oo. Default value is 2.0", default = '2')
parser.add_argument('-ec',type=str,help="When calling PRESTO rfifind function, zapped edge channel number for each coarse channel side. Defaule number is 20", default = '20')
args=parser.parse_args()

#--------------------------------------------------------------------------------------------------------------#

if args.obid:
    OBID = args.obid
else:
    print "Please input obsevation id by setting -o or --obid " 

#defaults for the fits dirs
if args.FITS_dir:
    FITS_directory = args.FITS_dir
else:
    #FITS_directory = '/group/mwaops/ICS_data/'
    FITS_directory = '/group/mwaops/vcs/'

#defaults for the work dirs
if args.WORK_dir:
    WORKDIR = args.WORK_dir
else:
    WORKDIR = '/scratch2/mwaops/pulsar/incoh_census'

#set PRESTO rfifind parameter
if args.rfitime:
    RFIT = args.rfitime
if args.ec:
    EDGE = args.ec

#--------------------------------------------------------------------------------------------------------------#

FITSDIR = FITS_directory + OBID + '/fits/*.fits'

#Make Obs_ID dir in '/group/mwaops/incoh_census'
if not os.path.exists(WORKDIR+'/'+OBID):
    os.mkdir("{0}/{1}/".format(WORKDIR,OBID))

#Call Nick's code to generate PSRs namelist
if not os.path.exists(WORKDIR+'/'+OBID+'/'+OBID+'_analytic_beam.txt'):
    findpsr_line = "find_pulsar_in_obs.py --all_volt --output {1}/{0}/ -o {0}\n".format(OBID,WORKDIR)
    submit_findpsr = subprocess.Popen(findpsr_line,shell=True,stdout=subprocess.PIPE)
    for line in submit_findpsr.stdout:
        print line

os.chdir(WORKDIR)

#Make fold & batch & rfi dir in '/group/mwaops/incoh_census/OBID'
if not os.path.exists(WORKDIR+'/'+OBID+'/fold/'):
    os.mkdir("{0}/{1}/fold/".format(WORKDIR,OBID)) 
if not os.path.exists(WORKDIR+'/'+OBID+'/batch/'):
    os.mkdir("{0}/{1}/batch/".format(WORKDIR,OBID))
if not os.path.exists(WORKDIR+'/'+OBID+'/rfi/'):
    os.mkdir("{0}/{1}/rfi/".format(WORKDIR,OBID))

#Remove RFI
jobid_rfi="none"
ECHAN=""
for i in range (0,24,1):
    e1=i*128
    e2=i*128+int(EDGE)-1
    e3=i*128+128-int(EDGE)
    e4=i*128+128-1
    ECHAN=ECHAN+str(e1)+':'+str(e2)+','+str(e3)+':'+str(e4)+','
#print ECHAN
if not os.path.exists(WORKDIR+'/'+OBID+'/rfi/'+OBID+'_t'+RFIT+'_ec'+EDGE+'_cl_rfifind.mask'):
    with open(WORKDIR+'/'+OBID+'/rfi/rfifind_'+OBID+'.batch','w') as batch_file:
        batch_line = "#!/bin/bash -l\n" +\
                     "#SBATCH --job-name=rfifind\n" +\
                     "#SBATCH --export=NONE\n" +\
                     "#SBATCH --partition=workq\n" +\
                     "#SBATCH --time=5:50:00\n" +\
                     "#SBATCH --nodes=1\n" +\
                     "#SBATCH --gid=mwaops\n" +\
                     "#SBATCH --account=mwaops\n" +\
                     "#SBATCH --output={0}/{1}/rfi/rfifind_{1}.out\n".format(WORKDIR,OBID) +\
                     "export OMP_NUM_THREADS=20\n" +\
                     "aprun -b -cc none -d 20 rfifind -ncpus 20 -o {0}/{1}/rfi/{1}_t{2}_ec{5}_cl -zapchan {3} -time {2} {4}\n".format(WORKDIR,OBID,RFIT,ECHAN,FITSDIR,EDGE)
        batch_file.write(batch_line)

    #Submit RFI removing batch
    rfi_batch=WORKDIR+'/'+OBID+'/rfi/rfifind_'+OBID+'.batch'
    submit_line = "sbatch --workdir={0} {1}\n".format(WORKDIR,rfi_batch)
    submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
    for line in submit_cmd.stdout:
        print line
        if "Submitted" in line:
            (word1,word2,word3,jobid_rfi) = line.split()


#Operate on each PSR
namelist=open(WORKDIR+'/'+OBID+'/'+OBID+'_analytic_beam.txt').readlines()
for line in namelist:
    if not line.startswith ('J'):
        continue

    psrline=line.split()
    psrname=psrline[0]
    enter=psrline[2] #fraction of obs when the pulsar enters the beam
    exit=psrline[3] #fraction of obs when the pulsar exits the beam
    print psrname
    
    PARDIR='/group/mwaops/xuemy/incoh_census/par_file/'+psrname+'.par' #Create Ephemerides file for each PSR
    if not os.path.isfile(PARDIR):
        psrcat_line = "psrcat -e {0} > {1}".format(psrname,PARDIR)
        create_parfile = subprocess.Popen(psrcat_line,shell=True,stdout=subprocess.PIPE)
        psrcat_line = "chmod 6774 {0}".format(PARDIR)
        create_parfile = subprocess.Popen(psrcat_line,shell=True,stdout=subprocess.PIPE)
        for line in create_parfile.stdout:
            print line

for line in namelist:
    if not line.startswith ('J'):
        continue
    psrline=line.split()
    psrname=psrline[0]
    enter=psrline[2] #fraction of obs when the pulsar enters the beam
    exit=psrline[3] #fraction of obs when the pulsar exits the beam
    print psrname
    #PARDIR=WORKDIR+'/par_file/'+psrname+'.par' #Create Ephemerides file for each PSR

    parfile = open(PARDIR).readlines()
    for ln,line in enumerate(parfile): #ln+1 is line number
        lnum=str(ln+1)
        word = line.split()
        if word[0] == 'DECJ':
            DECJ = word[1]
            #if DECJ[6] != ':':
            if len(DECJ) < 7:
                submitline="sed -i '/DECJ/s/{0}/{0}:00/' ".format(DECJ)
                submit_cmd = subprocess.Popen(submitline+PARDIR, shell=True, stdout=subprocess.PIPE)
        if word[0] == 'DM':
            DM = float(word[1])
        if word[0] == 'F0':
            F0 = float(word[1])
        if word[0] == 'BINARY':
            binary = word[1]
            if binary != 'DD' :
                submitline="sed -i '/BINARY/s/{0}/DD/' ".format(binary)
                submit_cmd = subprocess.Popen(submitline+PARDIR, shell=True, stdout=subprocess.PIPE)
        if word[0] == 'UNITS':
            units = word[1]
            if units != 'TDB' :
                submit_cmd = subprocess.Popen("sed -i '$d' "+PARDIR, shell=True, stdout=subprocess.PIPE)

    #Create fold batch for each PSR in the namelist
    os.system('')
    with open(OBID+'/batch/prepfold_'+psrname+'_'+OBID+'.batch','w') as batch_file:
        batch_line = "#!/bin/bash -l\n" +\
                     "#SBATCH --job-name=prepfold\n" +\
                     "#SBATCH --export=NONE\n" +\
                     "#SBATCH --partition=workq\n" +\
                     "#SBATCH --time=3:50:00\n" +\
                     "#SBATCH --gid=mwaops\n" +\
                     "#SBATCH --account=mwaops\n" +\
                     "#SBATCH --nodes=1\n" +\
                     "#SBATCH --output={0}/{2}/batch/prepfold_{1}_{2}.out\n".format(WORKDIR,psrname,OBID)
        batch_file.write(batch_line)
        if jobid_rfi != "none":
            batch_line = "#SBATCH --dependency=afterok:{0}\n".format(jobid_rfi)
            batch_file.write(batch_line)
        #batch_line = "aprun -cc none -d 20 prepfold -ncpus 20 -runavg -par /group/mwaops/xuemy/incoh_census/par_file/{0}.par -mask {1}/{2}/rfi/{2}_t2_nocl_rfifind.mask -o {1}/{2}/fold/{2} {3}\n".format(psrname,WORKDIR,OBID,FITSDIR)
        if F0 < 1.25 :
            batch_line = "export OMP_NUM_THREADS=20\n" +\
                         "aprun -cc none -d 20 prepfold -ncpus 20 -runavg -par /group/mwaops/xuemy/incoh_census/par_file/{0}.par -mask {1}/{2}/rfi/{2}_t{3}_ec{7}_cl_rfifind.mask -slow -o {1}/{2}/fold/{2} -nodmsearch -topo -start {4} -end {5} {6}\n".format(psrname,WORKDIR,OBID,RFIT,enter,exit,FITSDIR,EDGE)
        elif F0 < 2 :
            batch_line = "export OMP_NUM_THREADS=20\n" +\
                         "aprun -b -cc none -d 20 prepfold -ncpus 20 -runavg -par /group/mwaops/xuemy/incoh_census/par_file/{0}.par -mask {1}/{2}/rfi/{2}_t{3}_ec{7}_cl_rfifind.mask -n 100 -o {1}/{2}/fold/{2} -nodmsearch -topo -start {4} -end {5} {6}\n".format(psrname,WORKDIR,OBID,RFIT,enter,exit,FITSDIR,EDGE)
        elif F0 > 200 :
            batch_line = "export OMP_NUM_THREADS=20\n" +\
                         "aprun -b -cc none -d 20 prepfold -ncpus 20 -runavg -par /group/mwaops/xuemy/incoh_census/par_file/{0}.par -mask {1}/{2}/rfi/{2}_t{3}_ec{7}_cl_rfifind.mask -n 64 -o {1}/{2}/fold/{2} -nodmsearch -topo -start {4} -end {5} {6}\n".format(psrname,WORKDIR,OBID,RFIT,enter,exit,FITSDIR,EDGE)
        else:
            batch_line = "export OMP_NUM_THREADS=20\n" +\
                         "aprun -b -cc none -d 20 prepfold -ncpus 20 -runavg -par /group/mwaops/xuemy/incoh_census/par_file/{0}.par -mask {1}/{2}/rfi/{2}_t{3}_ec{7}_cl_rfifind.mask -o {1}/{2}/fold/{2}  -nodmsearch -topo -start {4} -end {5} {6}\n".format(psrname,WORKDIR,OBID,RFIT,enter,exit,FITSDIR,EDGE)
        batch_file.write(batch_line)

    jobid_fold="none"
    #Submit fold batch for each PSR
    fold_batch=OBID+'/batch/prepfold_'+psrname+'_'+OBID+'.batch'
    submit_line = "sbatch --workdir={0} {1}\n".format(WORKDIR,fold_batch)
    submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
    for line in submit_cmd.stdout:
        print line
        if "Submitted" in line:
            (word1,word2,word3,jobid_fold) = line.split()


    #Create nosearch fold batch for each PSR in the namelist
    os.system('')
    with open(OBID+'/batch/prepfold_nosearch'+psrname+'_'+OBID+'.batch','w') as batch_file:
        batch_line = "#!/bin/bash -l\n" +\
                     "#SBATCH --job-name=prepfold\n" +\
                     "#SBATCH --export=NONE\n" +\
                     "#SBATCH --partition=workq\n" +\
                     "#SBATCH --time=3:50:00\n" +\
                     "#SBATCH --gid=mwaops\n" +\
                     "#SBATCH --account=mwaops\n" +\
                     "#SBATCH --nodes=1\n" +\
                     "#SBATCH --output={0}/{2}/batch/prepfold_nosearch_{1}_{2}.out\n".format(WORKDIR,psrname,OBID)
        batch_file.write(batch_line)
        if F0 < 1.25 :
            batch_line = "export OMP_NUM_THREADS=20\n" +\
                         "aprun -b -cc none -d 20 prepfold -ncpus 20 -runavg -par /group/mwaops/xuemy/incoh_census/par_file/{0}.par -slow -o {1}/{2}/fold/{2}nsch -nosearch -topo -start {4} -end {5} {6}\n".format(psrname,WORKDIR,OBID,RFIT,enter,exit,FITSDIR,EDGE)
        elif F0 < 2 :
            batch_line = "export OMP_NUM_THREADS=20\n" +\
                         "aprun -b -cc none -d 20 prepfold -ncpus 20 -runavg -par /group/mwaops/xuemy/incoh_census/par_file/{0}.par -n 100 -o {1}/{2}/fold/{2}nsch -nosearch -topo -start {4} -end {5} {6}\n".format(psrname,WORKDIR,OBID,RFIT,enter,exit,FITSDIR,EDGE)
        elif F0 > 200 :
            batch_line = "export OMP_NUM_THREADS=20\n" +\
                         "aprun -b -cc none -d 20 prepfold -ncpus 20 -runavg -par /group/mwaops/xuemy/incoh_census/par_file/{0}.par -n 64 -o {1}/{2}/fold/{2}nsch -nosearch -topo -start {4} -end {5} {6}\n".format(psrname,WORKDIR,OBID,RFIT,enter,exit,FITSDIR,EDGE)
        else:
            batch_line = "export OMP_NUM_THREADS=20\n" +\
                         "aprun -b -cc none -d 20 prepfold -ncpus 20 -runavg -par /group/mwaops/xuemy/incoh_census/par_file/{0}.par -o {1}/{2}/fold/{2}nsch -nosearch -topo -start {4} -end {5} {6}\n".format(psrname,WORKDIR,OBID,RFIT,enter,exit,FITSDIR,EDGE)
        batch_file.write(batch_line)

    jobid_fold="none"
    #Submit fold batch for each PSR
    fold_batch=OBID+'/batch/prepfold_nosearch'+psrname+'_'+OBID+'.batch'
    submit_line = "sbatch --workdir={0} {1}\n".format(WORKDIR,fold_batch)
    submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
    for line in submit_cmd.stdout:
        print line
        if "Submitted" in line:
            (word1,word2,word3,jobid_fold) = line.split()
        


# convert output ps file into png using "convert" from ImageMagick
if not os.path.exists(WORKDIR+'/'+OBID+'/png/'):
    os.mkdir("{0}/{1}/png/".format(WORKDIR,OBID)) 
with open(OBID+'/batch/ps2png.batch','w') as batch_file:
    batch_line = "#!/bin/bash -l\n" +\
                 "#SBATCH --job-name=ps2png\n" +\
                 "#SBATCH --export=NONE\n" +\
                 "#SBATCH --partition=workq\n" +\
                 "#SBATCH --time=0:30:00\n" +\
                 "#SBATCH --gid=mwaops\n" +\
                 "#SBATCH --account=mwaops\n" +\
                 "#SBATCH --nodes=1\n" +\
                 "#SBATCH --output={0}/{1}/batch/ps2png.out\n".format(WORKDIR,OBID)
    batch_file.write(batch_line)
    if jobid_fold != "none":
        batch_line = "#SBATCH --dependency=afterok:{0}\n".format(jobid_fold)
        batch_file.write(batch_line)
    batch_line = "for psname in `ls {0}/{1}/fold/| grep pfd.ps `\n".format(WORKDIR,OBID) +\
                 "do\n" +\
                 "pngname=`echo ${psname%.*}`\n" +\
                 "aprun -b convert -density 150 -geometry 100% -rotate 90 -flatten {0}/{1}/fold/".format(WORKDIR,OBID) +\
                 "${psname}" + " {0}/{1}/png/".format(WORKDIR,OBID) + "${pngname}.png\n" +\
                 "done\n"
    batch_file.write(batch_line)
    batch_line = "chmod -R 6774 {0}/{1}\n".format(WORKDIR,OBID)
    batch_file.write(batch_line)

    





