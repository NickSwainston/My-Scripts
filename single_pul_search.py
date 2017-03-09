#! /usr/bin/env python

import subprocess
import os
import argparse
import urllib
import urllib2
import json
from time import sleep

#python /group/mwaops/nswainston/bin/blindsearch_pipeline.py -o 1133329792 -p 19:45:14.00_-31:47:36.00


def getmeta(service='obs', params=None):
    """
    Given a JSON web service ('obs', find, or 'con') and a set of parameters as
    a Python dictionary, return the RA and Dec in degrees from the Python dictionary.
    
    getmeta(service='obs', params=None)
    """
    BASEURL = 'http://mwa-metadata01.pawsey.org.au/metadata/'
    if params:
        data = urllib.urlencode(params)  # Turn the dictionary into a string with encoded 'name=value' pairs
    else:
        data = ''
    #Validate the service name
    if service.strip().lower() in ['obs', 'find', 'con']:
        service = service.strip().lower()
    else:
        print "invalid service name: %s" % service
        return
    #Get the data
    try:
        result = json.load(urllib2.urlopen(BASEURL + service + '?' + data))
    except urllib2.HTTPError as error:
        print "HTTP error from server: code=%d, response:\n %s" % (error.code, error.read())
        return
    except urllib2.URLError as error:
        print "URL or network error: %s" % error.reason
        return
    #Return the result dictionary
    return result
    
    
def add_database_function():
    batch_line ="#SBATCH --export=NONE\n" +\
                "#SBATCH --partition=workq\n" +\
                "#SBATCH --gid=mwaops\n" +\
                "#SBATCH --account=mwaops\n" +\
                "#SBATCH --nodes=1\n" +\
                "export OMP_NUM_THREADS=20\n" +\
                "ncpus=20\n" +\
                'aprun="aprun -b -n 1 -d $ncpus -q "\n' 
    return batch_line

parser = argparse.ArgumentParser(description="""
Does a blind search for a pulsar in MWA data using the galaxy supercomputer.
""")
parser.add_argument('-o','--observation',type=str,help='The observation ID of the fits file to be searched')
parser.add_argument('-p','--pulsar',type=str,help='The pulsar to be searched around their period and DM')
parser.add_argument('-m','--mode',type=str,help='There are three modes or steps to complete the pipeline. The first mode is to prepdata "p" which dedisperses the the fits files into .dat files. The second mode sort and search "s" which sorts the files it folders and performs a fft. The third mode is accel search "a" runs an accel search on them. The final mode is fold "f" which folds all possible candidates so they can be visaully inspected.')
parser.add_argument('-w','--work_dir',type=str,help='Work directory. Default: /scratch2/mwaops/nswainston/tara_candidates/')
parser.add_argument('-f','--fold',type=str,help='Fold option for the last mode')

#parser.add_argument('-o','--observation',action='store_true',help='')
args=parser.parse_args()

obsid = args.observation
#obsid =  1133329792
pulsar = args.pulsar
#19:45:14.00_-31:47:36.00


if args.work_dir:
    work_dir = args.work_dir
else:
    work_dir = '/scratch2/mwaops/pulsar/incoh_census/'





#-------------------------------------------------------------------------------------------------------------
if args.mode == "p" or args.mode == None:
    if not os.path.exists(work_dir+'/'+obsid+'/accel_searches/'):
        os.mkdir(work_dir+'/'+obsid+'/accel_searches/')
    work_dir = work_dir+'/'+obsid+'/accel_searches/'

    #Set up some directories and move to it
    if not os.path.exists(work_dir + "/" + pulsar):
        os.mkdir(work_dir + "/" + pulsar)
        print str(work_dir + "/" + pulsar)
    os.chdir(work_dir + "/" + pulsar)
    
    
    cmd = ['psrcat', '-c', 'DM', pulsar]
    output = subprocess.Popen(cmd,stdout=subprocess.PIPE).communicate()[0]
    temp = []
    lines = output.split('\n')
    for l in lines[4:-1]: 
        columns = l.split()
        if len(columns) > 1:
            dm = float(columns[1])
    
    
    #Get the centre freq channel and then run DDplan.py to work out the most effective DMs
    print "Obtaining metadata from http://mwa-metadata01.pawsey.org.au/metadata/ for OBS ID: " + str(obsid)
    beam_meta_data = getmeta(service='obs', params={'obs_id':obsid})
    channels = beam_meta_data[u'rfstreams'][u"0"][u'frequencies']
    minfreq = float(min(channels))
    maxfreq = float(max(channels))
    centrefreq = 1.28 * (minfreq + (maxfreq-minfreq)/2) #in MHz
    
    output = subprocess.Popen(['DDplan.py','-l',str(dm*0.99),'-d',str(dm*1.01),'-f',str(centrefreq),'-b','30.7200067160534','-t','0.0001','-n','3072'],stdout=subprocess.PIPE).communicate()
    subprocess.check_call("\n", shell=True)
    dm_list = []
    print output[0]
    lines = output[0].split('\n')
    for l in lines[13:-4]: 
        columns = l.split()
        dm_list.append(columns)
    
    #Submit a bunch some prepsubbands to create our .dat files
    job_id_list = []
    for dm_line in dm_list:
        with open('DM_' + dm_line[0] + '.batch','w') as batch_file:
            batch_line = "#!/bin/bash -l\n" +\
                         "#SBATCH --job-name=prepsubbands\n" +\
                         "#SBATCH --output=DM_" + dm_line[0] + ".out\n" +\
                         "#SBATCH --export=NONE\n" +\
                         "#SBATCH --partition=workq\n" +\
                         "#SBATCH --time=3:50:00\n" +\
                         "#SBATCH --gid=mwaops\n" +\
                         "#SBATCH --account=mwaops\n" +\
                         "#SBATCH --nodes=1\n" +\
                         "export OMP_NUM_THREADS=20\n" +\
                         "aprun -cc none -d 20 prepsubband -ncpus 20 -lodm " + str(dm_line[0]) +\
                            " -dmstep " + str(dm_line[2]) + " -numdms " + str(dm_line[4]) + \
                            " -o " + str(obsid) + " /scratch2/mwaops/vcs/" + str(obsid) + \
                            "/fits/" + str(obsid) + "*.fits"
            batch_file.write(batch_line)
            
        submit_line = 'sbatch DM_' + dm_line[0] + '.batch'
        submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
        for line in submit_cmd.stdout:
            print line
            if "Submitted" in line:
                (word1,word2,word3,jobid) = line.split()
                job_id_list.append(jobid)
    
    
    #make a job that simply restarts this program when prepsubbands are complete
    job_id_str = ""
    for i in job_id_list:
        job_id_str += ":" + str(i)
    with open('dependancy_prepsubbands.batch','w') as batch_file:
        batch_line = "#!/bin/bash -l\n" +\
                     "#SBATCH --job-name=dependancy\n" +\
                     "#SBATCH --output=dependancy.out\n" +\
                     "#SBATCH --export=NONE\n" +\
                     "#SBATCH --partition=workq\n" +\
                     "#SBATCH --time=0:05:00\n" +\
                     "#SBATCH --gid=mwaops\n" +\
                     "#SBATCH --account=mwaops\n" +\
                     "#SBATCH --nodes=1\n" +\
                     "#SBATCH --dependency=afterok" + job_id_str + "\n" +\
                     "python /group/mwaops/nswainston/bin/single_pul_search.py -o "\
                             + str(obsid) + " -p " + str(pulsar) + " -m s -w " + work_dir
        batch_file.write(batch_line)
            
    submit_line = 'sbatch dependancy_prepsubbands.batch'
    submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
    #print submit_cmd.stdout
    print "Sent off prepsubband jobs"
                
#-------------------------------------------------------------------------------------------------------------
elif args.mode == "s":
    #Makes 90 files to make this all a bit more managable and sorts the files.
    os.chdir(work_dir + "/" + pulsar )
    if not os.path.exists("over_3_png"):
        os.mkdir("over_3_png")
    if not os.path.exists("other_png"):
        os.mkdir("other_png")
   
    
    DIR=work_dir + "/" + pulsar + "/" 
    length = len(DIR)
    dirlist = [f for f in os.listdir(DIR) if os.path.isdir(os.path.join(DIR, f))]
    #dirlist =[x[0][length:] for x in os.walk(DIR)]

    all_files = os.listdir(work_dir + "/" + pulsar + "/")
    #Sort loop
    
    if not os.path.exists(work_dir + "/" + pulsar + "/" + "/batch"):
        os.mkdir(work_dir + "/" + pulsar + "/" + "/batch")
    if not os.path.exists(work_dir + "/" + pulsar + "/out"):
        os.mkdir(work_dir + "/" + pulsar + "/" + "/out")
    dir_files = os.listdir(work_dir + "/" + pulsar + "/")
    with open('batch/fft.batch','w') as batch_file:
        batch_line = "#!/bin/bash -l\n" +\
                     "#SBATCH --job-name=fft\n" +\
                     "#SBATCH --output=" + work_dir + "/" + pulsar + "/out/fft.out\n" +\
                     "#SBATCH --export=NONE\n" +\
                     "#SBATCH --partition=workq\n" +\
                     "#SBATCH --time=4:50:00\n" +\
                     "#SBATCH --gid=mwaops\n" +\
                     "#SBATCH --account=mwaops\n" +\
                     "#SBATCH --nodes=1\n" +\
                     "export OMP_NUM_THREADS=20\n" 
        batch_file.write(batch_line)            
        for f in dir_files:
            if f.endswith(".dat"):            
                batch_line = "aprun -b -cc none -d 20 realfft " + str(f) + "\n"
                batch_file.write(batch_line)
                
        batch_line = "single_pul_search.py -o " +\
                             str(obsid) + " -p " + str(pulsar) + " -m a -w " + work_dir +\
                             "/" + pulsar 
        batch_file.write(batch_line)
        
    submit_line = 'sbatch batch/fft.batch'
    submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
                
                
#-------------------------------------------------------------------------------------------------------------
elif args.mode == "a":
    #blindsearch_pipeline.py -o 1133329792 -p 19:45:14.00_-31:47:36.00 -m a -w /scratch2/mwaops/nswainston/tara_candidates//19:45:14.00_-31:47:36.00/1133329792/DM_058-060
    # sends off the accel search jobs
    #work_dir = work_dir +"/" + pulsar + "/" + obsid + "/" + d
    os.chdir(work_dir)
    
    cmd = ['psrcat', '-c', 'p0', pulsar]
    output = subprocess.Popen(cmd,stdout=subprocess.PIPE).communicate()[0]
    temp = []
    lines = output.split('\n')
    for l in lines[4:-1]: 
        columns = l.split()
        if len(columns) > 1:
            period = columns[1]
    freq = 1. / float(period)
    
    os.chdir(work_dir)
    dir_files = os.listdir(work_dir)
    job_id_list =[]
    for f in dir_files:
        if f.endswith(".fft"):
            with open('batch/accel_' + f[:-4] + '.batch','w') as batch_file:
                batch_line = "#!/bin/bash -l\n" +\
                             "#SBATCH --job-name=accel_" + f[:-4] + "\n" +\
                             "#SBATCH --output=" + work_dir + "/out/accel_" + f[:-4] + ".out\n" +\
                             "#SBATCH --time=11:59:00\n" 
                batch_file.write(batch_line)
                batch_file.write(add_database_function())
                batch_line = 'aprun accelsearch -ncpus 20 -flo '+ str(freq*0.99)+' -fhi '+ str(freq*1.01)+' -numharm 8 ' + f 
                #+ ' ' + work_dir
                             #TODO remove # when testing over
                             #TODO make a more robust workdir script
                batch_file.write(batch_line)
            
            submit_line = 'sbatch batch/accel_' + f[:-4] + '.batch'
            submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
            for line in submit_cmd.stdout:
                print line
                if "Submitted" in line:
                    (word1,word2,word3,jobid) = line.split()
                    job_id_list.append(jobid)
                #print submit_cmd.communicate()[0]
    
    job_id_str = ""
    for i in job_id_list:
        job_id_str += ":" + str(i)
    with open('batch/dependancy_accel.batch','w') as batch_file:
        batch_line = "#!/bin/bash -l\n" +\
                     "#SBATCH --job-name=dependancy\n" +\
                     "#SBATCH --output=out/depenndancy_accel.out\n" +\
                     "#SBATCH --export=NONE\n" +\
                     "#SBATCH --partition=workq\n" +\
                     "#SBATCH --time=0:05:00\n" +\
                     "#SBATCH --gid=mwaops\n" +\
                     "#SBATCH --account=mwaops\n" +\
                     "#SBATCH --nodes=1\n" +\
                     "#SBATCH --dependency=afterok" + job_id_str + "\n" +\
                     "python /group/mwaops/nswainston/bin/single_pul_search.py -o " + str(obsid) +\
                             " -p " + str(pulsar) + " -m f -w " + work_dir + "\n"
        batch_file.write(batch_line)   
    submit_line = 'sbatch batch/dependancy_accel.batch'
    submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
    #print submit_cmd.stdout
    print "Sent off accel jobs"
       
#-------------------------------------------------------------------------------------------------------------
elif args.mode == "f":
    os.chdir(work_dir)
    all_files = os.listdir(work_dir)
    with open('batch/fold_' + work_dir[-10:] + '.batch','w') as batch_file:
        batch_line = "#!/bin/bash -l\n" +\
                     "#SBATCH --job-name=fold_" + work_dir[-10:] + "\n" +\
                     "#SBATCH --output=" + work_dir + "/out/fold_" + work_dir[-10:] +".out\n" +\
                     "#SBATCH --export=NONE\n" +\
                     "#SBATCH --partition=workq\n" +\
                     "#SBATCH --time=12:00:00\n" +\
                     "#SBATCH --gid=mwaops\n" +\
                     "#SBATCH --account=mwaops\n" +\
                     "#SBATCH --nodes=1\n" +\
                     "export OMP_NUM_THREADS=20\n" 
        batch_file.write(batch_line)

        for fold_option in all_files:
            if fold_option.endswith("_ACCEL_200"):
                with open(fold_option,'rw') as accel_file:
                    cands = accel_file.readlines()
                for c in range(len(cands)):
                    i = len(cands[c])
                    if i == 1:
                        cand_num = c
                        break
                cand_list = []
                for c in cands[3:cand_num]:
                    line = c.split(" ")
                    line = filter(None, line)
                    if float(line[1]) > 3.:
                        cand_list.append(line[0])
                
                    for i in cand_list:
                        #TODO remove after testing 
                        batch_line = "aprun -b -cc none -d 20 prepfold -ncpus 20 -n 128 -nsub 128 -runavg -accelcand " +\
                                                 str(i) + " -accelfile " + fold_option + ".cand  -o " +\
                                                 fold_option[:-10] + " -topo " + fold_option[:-10] + ".dat\n" +\
                                     "ps_to_png.sh " + fold_option[:-10] + "_ACCEL_Cand_" + str(i) +\
                                                 ".pfd.ps\n" +\
                                     'chi=`sed "13q;d" '+ fold_option[:-10] + '_ACCEL_Cand_' + str(i) +\
                                                 '.pfd.bestprof`\n' +\
                                     "if [ ${chi:20:3} -ge 3 ]; then\n" +\
                                     "mv " +  fold_option[:-10] + "_ACCEL_Cand_" + str(i) + ".pfd.png ../over_3_png/"+\
                                             fold_option[:-10] + "_ACCEL_Cand_" + str(i) + ".pfd.png\n" +\
                                     'echo "over"\n' +\
                                     "fi\n" +\
                                     "if [ ${chi:20:3} -lt 3 ]; then\n" +\
                                     "mv " + fold_option[:-10] + "_ACCEL_Cand_" + str(i) + ".pfd.png ../other_png/"+\
                                             fold_option[:-10] + "_ACCEL_Cand_" + str(i) + ".pfd.png\n" +\
                                     'echo "under"\n' +\
                                     "fi\n"
                                     
                        print str(i)
                        batch_file.write(batch_line)
    submit_line = 'sbatch batch/fold_' + work_dir[-10:] + '.batch'
    submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
    print submit_cmd.communicate()[0]
    
        
    
            
