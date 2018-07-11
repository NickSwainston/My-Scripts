#! /usr/bin/env python

import subprocess
import os
import argparse
import urllib
import urllib2
import json
import time
import mwa_metadb_utils as meta

parser = argparse.ArgumentParser(description="""
Wraps the splice_psrfits.sh script to automate it. Should be run from the foulder containing the files.
""")
parser.add_argument('-o','--observation',type=str,help='The observation ID to be used.')
parser.add_argument('-i','--incoh',action='store_true',help='Use this check if there are and incoh files from the beamformer.')
parser.add_argument('-w','--work_dir',type=str,help='Working directory of the vcs files.')
args=parser.parse_args()

    
obsid = args.observation
chan_st = range(24) 
    
print "Obtaining metadata from http://mwa-metadata01.pawsey.org.au/metadata/ for OBS ID: " + str(obsid)
beam_meta_data = meta.getmeta(service='obs', params={'obs_id':obsid})
channels = beam_meta_data[u'rfstreams'][u"0"][u'frequencies']



chan_order=[]
for i in range(24):
    if channels[i] < 129:
        chan_order.append(channels[i])
    else:
        chan_order.append( channels[chan_st[-(channels[i] -128)]])
print chan_order

#move into working fdir
os.chdir(args.work_dir)

if args.incoh:
    submit_line = 'ls *'+str(args.observation)+'_ch'+str(channels[-1])+'_incoh* | wc -l'
else:
    submit_line = 'ls *'+str(args.observation)+'_ch'+str(channels[-1])+'* | wc -l'
print submit_line
submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
out_lines = submit_cmd.stdout
for l in out_lines:
    n_fits = l
    print n_fits
    if "incoh" in l:
        print "check"
    
if int(n_fits) == 0:
    new_bf = False
    chan_order=[]
    for i in range(24):
        if channels[i] < 129:
            chan_order.append(i+1)
        else:
            chan_order.append( chan_st[-(channels[i] -128)] + 1)
    print chan_order

    submit_line = 'ls *'+str(args.observation)+'_01* | wc -l'
    submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
    out_lines = submit_cmd.stdout
    for l in out_lines:
        n_fits = l
else:
    new_bf = True

print n_fits

for n in range(int(n_fits)):
    submit_line = submit_line_incoh = 'splice_psrfits '
    for ch in chan_order:
            if ch < 10:
                submit_line += '*'+str(obsid)+'_ch00'+str(ch)
                submit_line_incoh += '*'+str(obsid)+'_ch00'+str(ch)
            elif int(ch) < 100:
                submit_line += '*'+str(obsid)+'_ch0'+str(ch)
                submit_line_incoh += '*'+str(obsid)+'_ch0'+str(ch)
            else:
                submit_line += '*'+str(obsid)+'_ch'+str(ch)
                submit_line_incoh += '*'+str(obsid)+'_ch'+str(ch)
            if (int(n)+1) < 10:
                submit_line += '_000'+str(int(n)+1)+'.fits '
                submit_line_incoh += '_incoh_000'+str(int(n)+1)+'.fits '
            else:
                submit_line += '_00'+str(int(n)+1)+'.fits '
                submit_line_incoh += '_incoh_00'+str(int(n)+1)+'.fits '
    submit_line += 'temp_'+str(int(n)+1)
    submit_line_incoh += 'temp_incoh_'+str(int(n)+1)
    

    if args.incoh:
        print submit_line_incoh
        submit_cmd = subprocess.Popen(submit_line_incoh,shell=True,stdout=subprocess.PIPE)
        out_lines = submit_cmd.stdout        
        for l in out_lines:
            print l
        time.sleep(5)
        if n < 9:
            print 'temp_incoh_'+str(int(n)+1)+'_0001.fits'
            os.rename('temp_incoh_'+str(int(n)+1)+'_0001.fits',str(obsid)+'_incoh_000'+\
                      str(int(n)+1)+'.fits')
        else:
            os.rename('temp_incoh_'+str(int(n)+1)+'_0001.fits', str(obsid)+'_incoh_00'+\
                      str(int(n)+1)+'.fits')        
    
    print submit_line
    submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
    out_lines = submit_cmd.stdout
    for l in out_lines:
        print l
    time.sleep(5)
    if n < 9:
        print 'temp_'+str(int(n)+1)+'_0001.fits'
        os.rename('temp_'+str(int(n)+1)+'_0001.fits',str(obsid)+'_000'+str(int(n)+1)+'.fits')
    else:
        os.rename('temp_'+str(int(n)+1)+'_0001.fits', str(obsid)+'_00'+str(int(n)+1)+'.fits')
    
os.chdir("-")
