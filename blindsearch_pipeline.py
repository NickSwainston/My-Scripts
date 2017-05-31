#! /usr/bin/env python

import subprocess
import os
import argparse
import urllib
import urllib2
import json
from time import sleep

#python /group/mwaops/nswainston/bin/blindsearch_pipeline.py -o 1133329792 -p 19:45:14.00_-31:47:36.00
#python /group/mwaops/nswainston/bin/blindsearch_pipeline.py -o 1150234552 -p 00:34:08.8703_-07:21:53.409 --pulsar J0034-0721
#python /group/mwaops/nswainston/bin/blindsearch_pipeline.py -o 1099414416 -p 05:34:32_+22:00:53 --pulsar J0534+2200

#1163853320 47 tuck data


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
                "#SBATCH --gid=mwaops\n" +\
                "#SBATCH --account=mwaops\n" +\
                "#SBATCH --nodes=1\n" +\
                "export OMP_NUM_THREADS=8\n" +\
                "ncpus=8\n" +\
                'aprun="aprun -b -n 1 -d $ncpus -q "\n' +\
                'function run\n' +\
                '{\n' +\
                '    # run command and add relevant data to the job database\n' +\
                '    # 1st parameter is command to be run (e.g. wsclean)\n' +\
                '    # 2nd parameter is parameters to that command (e.g. "-j $ncpus")\n' +\
                '    # 3rd parameter is datadir\n' +\
                '    # 4th parameter is project\n' +\
                '    # 5th parameter is obsid\n' +\
                '    # 6th parameter is chans [optional]\n' +\
                '    if [ -z "$6" ]; then\n' +\
                '        rownum=`cmd_start.py $1 -a "$2" -d $3 -p $4 -o $5`\n' +\
                '    else\n' +\
                '        rownum=`cmd_start.py $1 -a "$2" -d $3 -p $4 -o $5 -c $6`\n' +\
                '    fi\n' +\
                '    $aprun $1 $2\n' +\
                '    errcode=$?\n' +\
                '    cmd_stop.py $rownum -e $errcode\n' +\
                '    echo "cmd_stop.py $rownum -e $errcode"\n' +\
                '    if [ "$errcode" != "0" ]; then\n' +\
                '        exit $errcode\n' +\
                '    fi\n' +\
                '}\n'
    return batch_line
    
    
def numout_calc(DIR):
    #DIR = '/group/mwaops/vcs/1133329792/pointings/19:45:14.00_-31:47:36.00/'

    dirlist =[]
    for file in os.listdir(DIR):
        if file.endswith(".fits"):
            dirlist.append(file)

    numout = 0 
    for d in dirlist:
        readfile_out = []
        submit_line = 'readfile ' + DIR + '/' + d
        submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
        for line in submit_cmd.stdout:
            temp = line.split('=')
            readfile_out.append(temp)
         
        array = []       
        for line in readfile_out:
            temp = []
            for r in line:
                rr = r
                rr = rr.replace(' ','')
                rr = rr.replace('\n','')
                temp.append(rr)
            array.append(temp)
            
        #print array
             
          
        for a in array:
            if a[0] == 'Timeperfile(sec)':
                subint = int(a[1])
        numout += int(subint * 1e4)

    if numout%2:
        numout += 1        
    return numout
    
def get_pulsar_dm_p(pulsar):
    #Gets the ra and dec from the output of PSRCAT
    cmd = ['psrcat', '-c', 'dm', pulsar]
    output = subprocess.Popen(cmd,stdout=subprocess.PIPE).communicate()[0]
    temp = []
    lines = output.split('\n')
    for l in lines[4:-1]: 
        columns = l.split()
        if len(columns) > 1:
            dm = columns[1]
    cmd = ['psrcat', '-c', 'p0', pulsar]
    output = subprocess.Popen(cmd,stdout=subprocess.PIPE).communicate()[0]
    temp = []
    lines = output.split('\n')
    for l in lines[4:-1]: 
        columns = l.split()
        if len(columns) > 1:
            p = columns[1]
    return [dm, p]
    
#-------------------------------------------------------------------------------------------------------------
def rfifind(obsid, pointing, work_dir, sub_dir,pulsar=None):
    #Set up some directories and move to it
    if not os.path.exists(work_dir + pointing):
            os.mkdir(work_dir + pointing)
    if not os.path.exists(work_dir + pointing + "/" + obsid): 
            os.mkdir(work_dir + pointing + "/" + obsid)
    if not pulsar == None:
        if not os.path.exists(work_dir + pointing + "/" + obsid + "/" + pulsar): 
            os.mkdir(work_dir + pointing + "/" + obsid + "/" + pulsar)
        os.chdir(work_dir + pointing + "/" + obsid + "/" + pulsar)
        sub_dir = pointing + "/" + obsid + "/" + pulsar + "/"
    else:
        os.chdir(work_dir + pointing + "/" + obsid)
        sub_dir = pointing + "/" + obsid + "/"
    
    if not os.path.exists(work_dir + sub_dir + "/batch"): 
            os.mkdir(work_dir + sub_dir+ "/batch")
    if not os.path.exists(work_dir + sub_dir + "/out"): 
            os.mkdir(work_dir + sub_dir + "/out")
            
    #send off rfi job
    with open('batch/rfifind.batch','w') as batch_file:
        batch_line = "#!/bin/bash -l\n" +\
                     "#SBATCH --partition=gpuq\n" +\
                     "#SBATCH --job-name=rfifind\n" +\
                     "#SBATCH --output=out/rfifind.out\n" +\
                     "#SBATCH --time=3:50:00\n" 
        batch_file.write(batch_line)
        batch_file.write(add_database_function())
        batch_line = "aprun -b -n 1 -d 8 -q rfifind -ncpus 8 -noclip -time 12.0 "+\
                        "-o " + str(obsid) + " -zapchan `/group/mwaops/bmeyers/code/misc/zapchan.py "+\
                        "-r -N 3072 -Z` /group/mwaops/vcs/" + str(obsid) + \
                        "/pointings/" + str(pointing) + "/" + str(obsid) + "*.fits\n"+\
                        "python /group/mwaops/nswainston/bin/blindsearch_pipeline.py -o "\
                          + str(obsid) + " -p " + str(pointing) + " -m p -w " + work_dir +\
                          " -s " +str(sub_dir)
        batch_file.write(batch_line)
    submit_line = 'sbatch batch/rfifind.batch'
    submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
    for line in submit_cmd.stdout:
        print line,
    return


#-------------------------------------------------------------------------------------------------------------
def prepdata(obsid, pointing, work_dir, sub_dir,pulsar=None):
    if not pulsar == None:
        os.chdir(work_dir + pointing + "/" + obsid + "/" + pulsar)
        sub_dir = pointing + "/" + obsid + "/" + pulsar + "/"
    else:
        os.chdir(work_dir + pointing + "/" + obsid)
        sub_dir = pointing + "/" + obsid + "/"
    
    #Get the centre freq channel and then run DDplan.py to work out the most effective DMs
    print "Obtaining metadata from http://mwa-metadata01.pawsey.org.au/metadata/ for OBS ID: " + str(obsid)
    beam_meta_data = getmeta(service='obs', params={'obs_id':obsid})
    channels = beam_meta_data[u'rfstreams'][u"0"][u'frequencies']
    minfreq = float(min(channels))
    maxfreq = float(max(channels))
    centrefreq = 1.28 * (minfreq + (maxfreq-minfreq)/2) #in MHz
    
    if not pulsar == None:
        dm, p = get_pulsar_dm_p(pulsar)
        output = subprocess.Popen(['DDplan.py','-l',str(float(dm) - 1.),'-d',str(float(dm) + 1.),'-f',str(centrefreq),'-b','30.7200067160534','-t','0.0001','-n','3072'],stdout=subprocess.PIPE).communicate()
    else:
        output = subprocess.Popen(['DDplan.py','-l','0','-d','300','-f',str(centrefreq),'-b','30.7200067160534','-t','0.0001','-n','3072'],stdout=subprocess.PIPE).communicate()
    subprocess.check_call("\n", shell=True)
    dm_list = []
    print output[0]
    lines = output[0].split('\n')
    for l in lines[13:-4]: 
        columns = l.split()
        dm_list.append(columns)
        
    #Calculates -numout for prepsubbands
    numout = numout_calc("/group/mwaops/vcs/" + str(obsid) + "/pointings/" + str(pointing) + "/")
    
    #Submit a bunch some prepsubbands to create our .dat files
    job_id_list = []
    for dm_line in dm_list: 
        dm_start = dm_line[0]
        dm_end = float(dm_line[2]) * float(dm_line[4]) + float(dm_start)
        while ( (dm_end - float(dm_start)) / float(dm_line[2])) > 500. :
            #dedisperse for only 500 steps
            with open('batch/DM_' + dm_start + '.batch','w') as batch_file:
                batch_line = "#!/bin/bash -l\n" +\
                             "#SBATCH --partition=gpuq\n" +\
                             "#SBATCH --job-name=prepsub_" + dm_start + "\n" +\
                             "#SBATCH --output=out/DM_" + dm_start + ".out\n" +\
                             "#SBATCH --time=3:50:00\n" 
                batch_file.write(batch_line)
                batch_file.write(add_database_function())
                batch_line = "aprun -b -n 1 -d 8 -q prepsubband -ncpus 8 -lodm " + str(dm_start) +\
                                " -dmstep " + str(dm_line[2]) + " -numdms 500 -numout " + str(numout) +\
                                " -o " + str(obsid) + " /group/mwaops/vcs/" + str(obsid) + \
                                "/pointings/" + str(pointing) + "/" + str(obsid) + "*.fits"
                """
                batch_line = "run prepsubband '-ncpus 8 -lodm " + str(dm_start) +\
                                " -dmstep " + str(dm_line[2]) + " -numdms 500 -numout " + str(numout) +\
                                " -o " + str(obsid) + " /group/mwaops/vcs/" + str(obsid) + \
                                "/pointings/" + str(pointing) + "/" + str(obsid) + "*.fits' " +\
                                work_dir + ' blindsearch ' + obsid
                """
                batch_file.write(batch_line)
                
            submit_line = 'sbatch batch/DM_' + dm_start + '.batch'
            submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
            for line in submit_cmd.stdout:
                print line,
                print 'batch/DM_' + dm_start + '.batch'
                if "Submitted" in line:
                    (word1,word2,word3,jobid) = line.split()
                    job_id_list.append(jobid)
            
            dm_start = str(float(dm_start) + (500. * float(dm_line[2])))
        steps = int((dm_end - float(dm_start)) / float(dm_line[2]))
        #last loop to get the <500 steps
        with open('batch/DM_' + dm_start + '.batch','w') as batch_file:
            batch_line = "#!/bin/bash -l\n" +\
                         "#SBATCH --partition=gpuq\n" +\
                         "#SBATCH --job-name=prepsub_" + dm_start + "\n" +\
                         "#SBATCH --output=out/DM_" + dm_start + ".out\n" +\
                         "#SBATCH --time=3:50:00\n" 
            batch_file.write(batch_line)
            batch_file.write(add_database_function())
            batch_line = "aprun -b -n 1 -d 8 -q prepsubband -ncpus 8 -lodm " + str(dm_start) +\
                                " -dmstep " + str(dm_line[2]) + " -numdms 500 -numout " + str(numout) +\
                                " -o " + str(obsid) + " -mask " + str(obsid) + "_rfifind.mask "+\
                                "/group/mwaops/vcs/" + str(obsid) + \
                                "/pointings/" + str(pointing) + "/" + str(obsid) + "*.fits"
            """
            batch_line = "run prepsubband '-ncpus 8 -lodm " + str(dm_start) +\
                            " -dmstep " + str(dm_line[2]) + " -numdms " + str(steps) + \
                            " -numout " + str(numout) +\
                            " -o " + str(obsid) + " /group/mwaops/vcs/" + str(obsid) + \
                            "/pointings/" + str(pointing) + "/" + str(obsid) + "*.fits' " +\
                            work_dir + ' blindsearch ' + obsid
            """
            batch_file.write(batch_line)
            
        submit_line = 'sbatch batch/DM_' + dm_start + '.batch'
        submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
        for line in submit_cmd.stdout:
            print line,
            if "Submitted" in line:
                (word1,word2,word3,jobid) = line.split()
                job_id_list.append(jobid)
    
    
    #make a job that simply restarts this program when all prepsubband jobs are complete
    print "Waiting 5 sec to make sure to the dependancy script works"
    sleep(5)
    job_id_str = ""
    for i in job_id_list:
        job_id_str += ":" + str(i)
    with open('batch/dependancy_prepsubbands.batch','w') as batch_file:
        batch_line = "#!/bin/bash -l\n" +\
                     "#SBATCH --job-name=dependancy\n" +\
                     "#SBATCH --output=out/dependancy_prepsubbands.out\n" +\
                     "#SBATCH --export=NONE\n" +\
                     "#SBATCH --partition=workq\n" +\
                     "#SBATCH --time=0:05:00\n" +\
                     "#SBATCH --gid=mwaops\n" +\
                     "#SBATCH --account=mwaops\n" +\
                     "#SBATCH --nodes=1\n" +\
                     "#SBATCH --dependency=afterok" + job_id_str + "\n" +\
                     'aprun -b -n 1 -d 8 -q realfft ' + str(obsid) + '_DM0.00.dat\n'+\
                     "accelsearch -numharm 4 -zmax 0 " +str(obsid) + "_DM0.00.fft\n"+\
                     "#python /group/mwaops/nswainston/bin/blindsearch_pipeline.py -o "\
                          + str(obsid) + " -p " + str(pointing) + " -m s -w " + work_dir +\
                          " -s " +str(sub_dir)
        batch_file.write(batch_line)
        if not pulsar == None:
            batch_line = " --pulsar " + pulsar
            batch_file.write(batch_line)
        
            
    submit_line = 'sbatch batch/dependancy_prepsubbands.batch'
    submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
    for line in submit_cmd.stdout:
            print line,
    print "Sent off prepsubband jobs"
    return
                
#-------------------------------------------------------------------------------------------------------------
def sort_fft(obsid, pointing, work_dir, sub_dir, pulsar=None):
    #Makes 90 files to make this all a bit more managable and sorts the files.
    os.chdir(work_dir + "/" + sub_dir)
    if not os.path.exists("over_3_png"):
        os.mkdir("over_3_png")
    if not os.path.exists("other_png"):
        os.mkdir("other_png")
        
    os.chdir(work_dir + "/" + sub_dir)
    if pulsar==None:
        for i in range(4):
            if not os.path.exists("DM_00" + str(i*2) + "-00" + str((i+1)*2)):
                os.mkdir("DM_00" + str(i*2) + "-00" + str((i+1)*2))
        if not os.path.exists("DM_008-010"):
            os.mkdir("DM_008-010")
        for i in range(5,49):
            if not os.path.exists("DM_0" + str(i*2) + "-0" + str((i+1)*2)):
                os.mkdir("DM_0" + str(i*2) + "-0" + str((i+1)*2))
        if not os.path.exists("DM_098-100"):
            os.mkdir("DM_098-100")
        for i in range(50,90):
            if not os.path.exists("DM_" + str(i*2) + "-" + str((i+1)*2)):
                os.mkdir("DM_" + str(i*2) + "-" + str((i+1)*2))

    
    DIR=work_dir + sub_dir
    length = len(DIR)
    if pulsar==None:
        dirlist = [f for f in os.listdir(DIR) if os.path.isdir(os.path.join(DIR, f))]
        for d in dirlist:
            if not d.startswith("DM"):
                dirlist.remove(d)
    else:
        dirlist = ['.']

    all_files = os.listdir(DIR+ "/")
                
    for d in dirlist:
        #i and j are the start and stop dm to help with sorting
        #for weird bug where batch isn't removed from dirlist
        if d == 'batch':
            i = None
        else:
            i = int(d[3:6])
            if d.endswith("batch"):
                j = int(d[7:-6])
            elif d.endswith("out"):
                j = int(d[7:-4])
            else:
                j = int(d[7:])
                
            for f in all_files:
                if ( f.endswith(".dat") or f.endswith(".inf") ) and not f.endswith('rfifind.inf'):
                    temp = f.split("DM")
                    if float(i) <= float(temp[1][:-4]) < float(j):
                        #print temp[2][:-4]
                        os.rename(work_dir + sub_dir + "/" + str(f),work_dir + sub_dir \
                                  + "/" + str(d) + "/" + str(f))
    
    
    #Send off jobs
    
    job_id_list =[]
    for d in dirlist:
        if pulsar == None:
            if not os.path.exists(work_dir + sub_dir + "/" + d + "/batch"):
                os.mkdir(work_dir + sub_dir + "/" + d + "/batch")
            if not os.path.exists(work_dir + sub_dir + "/" + d + "/out"):
                os.mkdir(work_dir + sub_dir + "/" + d + "/out")
            os.chdir(work_dir + sub_dir + "/" + d)
            dir_files = os.listdir(work_dir + sub_dir + "/" + d + "/")
        else:
            dir_files = os.listdir(work_dir + sub_dir + "/")
        with open('batch/fft' + d + '.batch','w') as batch_file:
            batch_line = "#!/bin/bash -l\n" +\
                         "#SBATCH --partition=gpuq\n" +\
                         "#SBATCH --job-name=fft_" + d + "\n" +\
                         "#SBATCH --output=" + work_dir + sub_dir +\
                                           "/" + d + "/out/fft_" + d + ".out\n" +\
                         "#SBATCH --time=4:50:00\n" 
            batch_file.write(batch_line)   
            batch_file.write(add_database_function())         
            for f in dir_files:
                if f.endswith(".dat"):            
                    batch_line = 'aprun -b -n 1 -d 8 -q realfft ' + str(f) + '\n'
                    #batch_line = 'run realfft "' + str(f) + '" ' + work_dir + ' blindsearch ' + obsid +'\n'
                    batch_file.write(batch_line)
            
        submit_line = 'sbatch batch/fft' + d + '.batch'
        submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
        for line in submit_cmd.stdout:
                print line,
                if "Submitted" in line:
                    (word1,word2,word3,jobid) = line.split()
                    job_id_list.append(jobid)
                #print submit_cmd.communicate()[0]
    
    os.chdir(work_dir + "/" + sub_dir)
    
    sleep(1)
    job_id_str = ""
    for i in job_id_list:
        job_id_str += ":" + str(i)
    with open('batch/dependancy_fft.batch','w') as batch_file:
        batch_line = "#!/bin/bash -l\n" +\
                     "#SBATCH --job-name=dependancy\n" +\
                     "#SBATCH --output=out/dependancy_fft.out\n" +\
                     "#SBATCH --export=NONE\n" +\
                     "#SBATCH --partition=workq\n" +\
                     "#SBATCH --time=0:05:00\n" +\
                     "#SBATCH --gid=mwaops\n" +\
                     "#SBATCH --account=mwaops\n" +\
                     "#SBATCH --nodes=1\n" +\
                     "#SBATCH --dependency=afterok" + job_id_str + "\n" +\
                     "python /group/mwaops/nswainston/bin/blindsearch_pipeline.py -o " +\
                                 str(obsid) + " -p " + str(pointing) + " -m a -w " + work_dir +\
                                 " -s " + str(sub_dir) 
        batch_file.write(batch_line) 
        if not pulsar == None:
            batch_line = " --pulsar " + pulsar
            batch_file.write(batch_line)
        else:
            batch_line = " -d 0"
            batch_file.write(batch_line)
    submit_line = 'sbatch batch/dependancy_fft.batch'
    submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
    #print submit_cmd.stdout
    print "Sent off fft jobs"
    return
                
                
#-------------------------------------------------------------------------------------------------------------
def accel(obsid, pointing, work_dir, sub_dir, dm_i, pulsar=None):
    #blindsearch_pipeline.py -o 1133329792 -p 19:45:14.00_-31:47:36.00 -m a -w /scratch2/mwaops/nswainston/tara_candidates//19:45:14.00_-31:47:36.00/1133329792/DM_058-060
    # sends off the accel search jobs
    #sub_dir = pointing + "/" + obsid + "/"
    
    if 0 <= dm_i <= 3:
        dm_file = "DM_00" + str(dm_i*2) + "-00" + str((dm_i+1)*2)
    if dm_i == 4:
        dm_file = "DM_008-010"
    if 5 <= dm_i <= 48:
        dm_file = "DM_0" + str(dm_i*2) + "-0" + str((dm_i+1)*2)
    if dm_i == 49:
        dm_file = "DM_098-100"
    if 50 <= dm_i <= 89:
        dm_file = "DM_" + str(dm_i*2) + "-" + str((dm_i+1)*2)
    
    if pulsar == None:
        DIR = work_dir + str(sub_dir) + dm_file
    else:
        DIR = work_dir + str(sub_dir)
        dm, p = get_pulsar_dm_p(pulsar)
    os.chdir(DIR)
    dir_files = os.listdir(DIR)
    #dir_files = ['1150234552_DM10.92.fft']
    job_id_list =[]
    for f in dir_files:
        if f.endswith(".fft"):
            with open('batch/accel_' + f[:-4] + '.batch','w') as batch_file:
                batch_line = "#!/bin/bash -l\n" +\
                             "#SBATCH --partition=workq\n" +\
                             "#SBATCH --job-name=accel_" + f[13:-4] + "\n" +\
                             "#SBATCH --output=" + DIR + "/out/accel_" + f[:-4] + ".out\n" +\
                             "#SBATCH --time=6:00:00\n" 
                batch_file.write(batch_line)
                batch_file.write(add_database_function())
                if pulsar == None:
                    batch_line = 'run accelsearch "-ncpus 8 -flo 1 -fhi 500 -numharm 8 ' + f + '" ' +\
                                         DIR[:len(work_dir)] + ' ' + DIR[len(work_dir):] + ' ' + obsid 
                    batch_file.write(batch_line)
                else:
                    
                    batch_line = 'run accelsearch "-ncpus 8 -flo ' + str(1./(float(p)*1.15)) + ' -fhi ' +\
                                         str(1./(float(p)*0.85)) + ' -numharm 8 ' + f + '" ' +\
                                         DIR[:len(work_dir)] + ' ' + DIR[len(work_dir):] + ' ' + obsid 
                    """
                    batch_line = 'run accelsearch "-ncpus 8 -flo 1 -fhi 500 -numharm 8 ' + f + '" ' +\
                                         DIR[:len(work_dir)] + ' ' + DIR[len(work_dir):] + ' ' + obsid 
                    """
                    batch_file.write(batch_line)
            
            submit_line = 'sbatch batch/accel_' + f[:-4] + '.batch'
            submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
            for line in submit_cmd.stdout:
                print line,
                if "Submitted" in line:
                    (word1,word2,word3,jobid) = line.split()
                    job_id_list.append(jobid)
                #print submit_cmd.communicate()[0]
    sleep(1)
    print "Dependancy job below"
    job_id_str = ""
    for i in job_id_list:
        job_id_str += ":" + str(i)
    with open('batch/dependancy_accel.batch','w') as batch_file:
        batch_line = "#!/bin/bash -l\n" +\
                     "#SBATCH --job-name=dependancy\n" +\
                     "#SBATCH --output=out/dependancy_accel.out\n" +\
                     "#SBATCH --export=NONE\n" +\
                     "#SBATCH --partition=workq\n" +\
                     "#SBATCH --time=0:05:00\n" +\
                     "#SBATCH --gid=mwaops\n" +\
                     "#SBATCH --account=mwaops\n" +\
                     "#SBATCH --nodes=1\n" +\
                     "#SBATCH --dependency=afterok" + job_id_str + "\n" +\
                     "python /group/mwaops/nswainston/bin/blindsearch_pipeline.py -o " + str(obsid) +\
                             " -p " + str(pointing) + " -m f -w " + work_dir + " -s " + str(sub_dir)
        batch_file.write(batch_line)  
        if pulsar == None:
             batch_line = " -d " + str(dm_i)
             batch_file.write(batch_line)
        else:
            batch_line = " --pulsar " + str(pulsar)
            batch_file.write(batch_line)
    submit_line = 'sbatch batch/dependancy_accel.batch'
    submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
    for line in submit_cmd.stdout:
                print line,
    print "Sent off accel jobs"
    return
       
#-------------------------------------------------------------------------------------------------------------
def fold(obsid, pointing, work_dir, sub_dir, dm_i, pulsar = None):
    if 0 <= dm_i <= 3:
        dm_file = "DM_00" + str(dm_i*2) + "-00" + str((dm_i+1)*2)
    if dm_i == 4:
        dm_file = "DM_008-010"
    if 5 <= dm_i <= 48:
        dm_file = "DM_0" + str(dm_i*2) + "-0" + str((dm_i+1)*2)
    if dm_i == 49:
        dm_file = "DM_098-100"
    if 50 <= dm_i <= 89:
        dm_file = "DM_" + str(dm_i*2) + "-" + str((dm_i+1)*2)
    
    if pulsar == None:    
        DIR = work_dir + str(sub_dir) + dm_file
    else:
        DIR = work_dir + str(sub_dir)[:-1]
    os.chdir(DIR)
    all_files = os.listdir(DIR)
    """
    with open('batch/fold_' + DIR[-10:] + '.batch','w') as batch_file:
        batch_line = "#!/bin/bash -l\n" +\
                     "#SBATCH --partition=gpuq\n" +\
                     "#SBATCH --job-name=fold_" + DIR[-10:] + "\n" +\
                     "#SBATCH --output=" + DIR + "/out/fold_" + DIR[-10:] +".out\n" +\
                     "#SBATCH --time=12:00:00\n" 
        batch_file.write(batch_line)
        batch_file.write(add_database_function())
    """
    
    cand_list = []
    for fold_option in all_files:
        if fold_option.endswith("_ACCEL_200"):
            with open(fold_option,'rw') as accel_file:
                cands = accel_file.readlines()
            for c in range(len(cands)):
                i = len(cands[c])
                if i == 1:
                    cand_num = c
                    break
            
            #Stops including candidates over 3 sigma after 100 #TODO make a more robust system
            cand_count = 0 
            for c in cands[3:cand_num]:
                line = c.split(" ")
                line = filter(None, line)
                if float(line[1]) > 3. and cand_count < 100 and float(line[3]) > 60.:
                    cand_list.append([fold_option,line[0]])
                    cand_count += 1
            
    print "Number of cands in this file: " + str(len(cand_list))
    fold_num = 1
    job_id_list =[]
          
    #through some stuffing around sort the fold into 100 folds per job
    for i in range(len(cand_list)):
        if (i == 0) or ((i % 100) == 0):
            #print "here" + str(i)
            batch_file = open('batch/fold_' + DIR[-10:] + '_' + str(fold_num) + '.batch','w')
            batch_line = "#!/bin/bash -l\n" +\
                         "#SBATCH --partition=workq\n" +\
                         "#SBATCH --job-name=fold_" + str(fold_num) + '_'+ DIR[-10:] + "\n" +\
                         "#SBATCH --output=" + DIR + "/out/fold_" + DIR[-10:] + \
                                            '_' + str(fold_num) +".out\n" +\
                         "#SBATCH --time=12:00:00\n" 
            batch_file.write(batch_line)
            batch_file.write(add_database_function())
            """
            batch_line = "run prepfold '-ncpus 8 -n 128 -nsub 128 -runavg -accelcand " +\
                                str(i) + " -accelfile " + fold_option + ".cand  -o " +\
                                fold_option[:-10] + " -topo " + fold_option[:-10] + ".dat' " +\
                                " " + work_dir + " blindsearch " + obsid + "\n" +\
            """ 
            batch_line = "aprun -b -n 1 -d 8 -q prepfold -ncpus 20 -n 128 -nsub 128 -accelcand " +\
                         str(cand_list[i][1]) + " -accelfile " + cand_list[i][0] + ".cand  -o " +\
                         cand_list[i][0][:-10] + " -topo " + cand_list[i][0][:-10] + ".dat\n" 
            batch_file.write(batch_line)
            batch_file.close()
        else:
            batch_file = open('batch/fold_' + DIR[-10:] + '_' + str(fold_num) + '.batch','a')
            """
            batch_line = "run prepfold '-ncpus 8 -n 128 -nsub 128 -runavg -accelcand " +\
                                str(i) + " -accelfile " + fold_option + ".cand  -o " +\
                                fold_option[:-10] + " -topo " + fold_option[:-10] + ".dat' " +\
                                " " + work_dir + " blindsearch " + obsid + "\n" +\
            """ 
            batch_line = "aprun -b -n 1 -d 20 -q prepfold -ncpus 20 -n 128 -nsub 128 -accelcand "+\
                         str(cand_list[i][1]) + " -accelfile " + cand_list[i][0] + ".cand  -o " +\
                         cand_list[i][0][:-10] + " -topo " + cand_list[i][0][:-10] + ".dat\n" 
            batch_file.write(batch_line)
            batch_file.close()
        if ((i+1) % 100) == 0:
             
            submit_line = 'sbatch batch/fold_' + DIR[-10:] + '_' + str(fold_num) +  '.batch'
            submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
            for line in submit_cmd.stdout:
                print line,
                if "Submitted" in line:
                    (word1,word2,word3,jobid) = line.split()
                    job_id_list.append(jobid)
            fold_num += 1
    if not ((len(cand_list)+1) % 100) == 0: 
        submit_line = 'sbatch batch/fold_' + DIR[-10:] + '_' + str(fold_num) +  '.batch'
        submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
        for line in submit_cmd.stdout:
                print line,
                if "Submitted" in line:
                    (word1,word2,word3,jobid) = line.split()
                    job_id_list.append(jobid)
    
    

    sleep(1)
    job_id_str = ""
    for i in job_id_list:
        job_id_str += ":" + str(i)
    
    with open('batch/dependancy_fold_' + DIR[-10:] + '.batch','w') as batch_file:
        batch_line = "#!/bin/bash -l\n" +\
                     "#SBATCH --job-name=dependancy_fold\n" +\
                     "#SBATCH --output=out/dependancy_fold_" +  DIR[-10:] + ".out\n" +\
                     "#SBATCH --export=NONE\n" +\
                     "#SBATCH --partition=workq\n" +\
                     "#SBATCH --time=2:05:00\n" +\
                     "#SBATCH --gid=mwaops\n" +\
                     "#SBATCH --account=mwaops\n" +\
                     "#SBATCH --nodes=1\n" +\
                     'echo "Searching for a profile with a sigma greater than 3"\n' +\
                     'count=0\n' +\
                     'total=`ls *.ps | wc -l`\n' +\
                     'for i in $(ls *.ps); do\n' +\
                     'if (( $count % 100 == 0 )); then\n' +\
                     'echo "$count / $total searched"\n' +\
                     'fi\n' +\
                     'chi=`sed "13q;d" ${i%.ps}.bestprof`\n' +\
                     "if [ ${chi:20:3} -ge 3 ]; then\n" +\
                     'ps_to_png.sh ${i}\n' +\
                     'mv "${i%.ps}".png ../over_3_png/"${i%.ps}".png\n' +\
                     'echo "${i%.ps}.png is over 3"\n' +\
                     "fi\n" +\
                     "count=$(($count+1))\n" +\
                     "done\n" +\
                     "python /group/mwaops/nswainston/bin/blindsearch_pipeline.py -o " +\
                                 str(obsid) + " -p " + str(pointing) + " -m a -w " + work_dir +\
                                 " -s " + str(sub_dir) + " -d " + str(dm_i + 1) 
        batch_file.write(batch_line)   
    submit_line = 'sbatch batch/dependancy_fold_' + DIR[-10:] + '.batch'
    submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
    print submit_cmd.communicate()[0],
    
    
    return


parser = argparse.ArgumentParser(description="""
Does a blind search for a pulsar in MWA data using the galaxy supercomputer.
""")
parser.add_argument('-o','--observation',type=str,help='The observation ID of the fits file to be searched')
parser.add_argument('-p','--pointing',type=str,help='The pointing of the fits file to be searched')
parser.add_argument('-m','--mode',type=str,help='There are three modes or steps to complete the pipeline. The first mode is to prepdata "p" which dedisperses the the fits files into .dat files. The second mode sort and search "s" which sorts the files it folders and performs a fft. The third mode is accel search "a" runs an accel search on them. The final mode is fold "f" which folds all possible candidates so they can be visaully inspected.')
parser.add_argument('-w','--work_dir',type=str,help='Work directory. Default: /scratch2/mwaops/nswainston/tara_candidates/')
parser.add_argument('-s','--sub_dir',type=str,help='Used by the program to keep track of the sub directory its using')
parser.add_argument('-d','--dm_file_int',type=int,help='Used by the program to keep track DM file being used to stagger the jobs and not send off over 9000 jobs.')
parser.add_argument('--pulsar',type=str,help="Used to search for a known pulsar by inputing it's Jname. The code then looks within 1 DM and 15% of the pulsar's period.")
args=parser.parse_args()


if args.work_dir:
    w_d = args.work_dir
else:
    w_d = '/group/mwaops/nswainston/blindsearch/'

obs = args.observation
#obsid =  1133329792
point = args.pointing
#19:45:14.00_-31:47:36.00
s_d = args.sub_dir

if args.mode == "r" or args.mode == None:
    rfifind(obs, point, w_d, s_d,args.pulsar)
if args.mode == "p":
    prepdata(obs, point, w_d, s_d,args.pulsar)
elif args.mode == "s":
    sort_fft(obs, point, w_d, s_d,args.pulsar)
elif args.mode == "a":
    accel(obs, point, w_d, s_d,args.dm_file_int,args.pulsar)
elif args.mode == "f":
    fold(obs, point, w_d, s_d,args.dm_file_int,args.pulsar)
    
        
    
#blindsearch_pipeline.py -o 1166459712 -p 06:30:00.0_-28:34:00.0
