#! /usr/bin/env python

import subprocess
import os
import argparse
import urllib
import urllib2
import json
import time

parser = argparse.ArgumentParser(description="""
Wraps the splice_psrfits.sh script to automate it. Should be run from the foulder containing the files.
""")
parser.add_argument('-o','--observation',type=str,help='The observation ID to be used.')
args=parser.parse_args()

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
    
obsid = args.observation
chan_st = range(24) 
    
print "Obtaining metadata from http://mwa-metadata01.pawsey.org.au/metadata/ for OBS ID: " + str(obsid)
beam_meta_data = getmeta(service='obs', params={'obs_id':obsid})
channels = beam_meta_data[u'rfstreams'][u"0"][u'frequencies']



chan_order=[]
for i in range(24):
    if channels[i] < 129:
        chan_order.append(channels[i])
    else:
        chan_order.append( channels[chan_st[-(channels[i] -128)]])
print chan_order

submit_line = 'ls *'+str(args.observation)+'_ch'+str(channels[0])+'* | wc -l'
submit_cmd = subprocess.Popen(submit_line,shell=True,stdout=subprocess.PIPE)
out_lines = submit_cmd.stdout
for l in out_lines:
    n_fits = l
    
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
    submit_line = 'splice_psrfits '
    for ch in chan_order:
        if new_bf:
            submit_line += '*'+str(obsid)+'_ch'+str(ch)+'_*0'+str(int(n)+1)+'.fits '
        else:
            if ch < 10:
                submit_line += '*'+str(obsid)+'_0'+str(ch)+'_*0'+str(int(n)+1)+'.fits '
            else:
                submit_line += '*'+str(obsid)+'_'+str(ch)+'_*0'+str(int(n)+1)+'.fits '
    submit_line += 'temp_'+str(int(n)+1)
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


