import os
import sys
import math
import argparse
import urllib
import urllib2
import json
import subprocess
import numpy as np
from astropy.io import fits
from astropy.coordinates import SkyCoord
from astropy import units as u
from astropy.table import Table
from astropy.time import Time
from mwapy.pb import primary_beam
import ephem
from mwapy import ephem_utils,metadata
import matplotlib.pyplot as plt
import matplotlib.path as mpath
import matplotlib.patches as mpatches
import matplotlib.tri as tri
import matplotlib.cm as cm
#from mpl_toolkits.basemap import Basemap

def get_beam_power(obsid_data,
                   sources,
                   dt=296,
                   centeronly=True,
                   verbose=False,
                   min_power=0.6):
    """
    obsid_data = [obsid,ra, dec, time, delays,centrefreq, channels]
    sources=[names,coord1,coord2] #astropy table coloumn names

    Calulates the power (gain at coordinate/gain at zenith) for each source and if it is above
    the min_power then it outputs it to the text file.

    """
    #print "Calculating beam power"
    obsid,ra, dec, time, delays,centrefreq, channels = obsid_data
    
    #starttimes=np.arange(0,time,dt)
    starttimes=np.arange(0,time,time)
    stoptimes=starttimes+dt
    stoptimes[stoptimes>time]=time
    Ntimes=len(starttimes)
    midtimes=float(obsid)+0.5*(starttimes+stoptimes)

    mwa = ephem_utils.Obs[ephem_utils.obscode['MWA']]
    # determine the LST
    observer = ephem.Observer()
    # make sure no refraction is included
    observer.pressure = 0
    observer.long = mwa.long / ephem_utils.DEG_IN_RADIAN
    observer.lat = mwa.lat / ephem_utils.DEG_IN_RADIAN
    observer.elevation = mwa.elev

    if not centeronly:
        PowersX=np.zeros((len(sources),
                             Ntimes,
                             len(channels)))
        PowersY=np.zeros((len(sources),
                             Ntimes,
                             len(channels)))
        # in Hz
        frequencies=np.array(channels)*1.28e6
    else:
        PowersX=np.zeros((len(sources),
                             Ntimes,1))
        PowersY=np.zeros((len(sources),
                             Ntimes,1))
        frequencies=[centrefreq]

    RAs=np.array([x[0] for x in sources])
    Decs=np.array([x[1] for x in sources])
    if len(RAs)==0:
        sys.stderr.write('Must supply >=1 source positions\n')
        return None
    if not len(RAs)==len(Decs):
        sys.stderr.write('Must supply equal numbers of RAs and Decs\n')
        return None
    for itime in xrange(Ntimes):
        obstime = Time(midtimes[itime],format='gps',scale='utc')
        observer.date = obstime.datetime.strftime('%Y/%m/%d %H:%M:%S')
        LST_hours = observer.sidereal_time() * ephem_utils.HRS_IN_RADIAN

        HAs = -RAs + LST_hours * 15
        Azs, Alts = ephem_utils.eq2horz(HAs, Decs, mwa.lat)
        # go from altitude to zenith angle
        theta=np.radians(90-Alts)
        phi=np.radians(Azs)
        
        for ifreq in xrange(len(frequencies)):
            rX,rY=primary_beam.MWA_Tile_analytic(theta, phi,
                                                 freq=frequencies[ifreq], delays=delays,
                                                 zenithnorm=True,
                                                 power=True)
            PowersX[:,itime,ifreq]=rX
            PowersY[:,itime,ifreq]=rY

    #Power [#sources, #times, #frequencies]
    Powers=0.5*(PowersX+PowersY)
                 
    return Powers

# Append the service name to this base URL, eg 'con', 'obs', etc.
BASEURL = 'http://mwa-metadata01.pawsey.org.au/metadata/'

def getmeta(service='obs', params=None):
  """Given a JSON web service ('obs', find, or 'con') and a set of parameters as
     a Python dictionary, return a Python dictionary containing the result.
  """
  if params:
    data = urllib.urlencode(params)  # Turn the dictionary into a string with encoded 'name=value' pairs
  else:
    data = ''
  print data
  #             Validate the service name
  if service.strip().lower() in ['obs', 'find', 'con']:
    service = service.strip().lower()
  else:
    print "invalid service name: %s" % service
    return
  #             Get the data
  try:
    result = json.load(urllib2.urlopen(BASEURL + service + '?' + data))
  except urllib2.HTTPError as error:
    print "HTTP error from server: code=%d, response:\n %s" % (error.code, error.read())
    return
  except urllib2.URLError as error:
    print "URL or network error: %s" % error.reason
    return
  #            Return the result dictionary
  return result



plt.figure()
plt.rc("font", size=8)
ax=plt.subplot(111, projection = 'mollweide')


data_PCAT=open('/group/mwaops/xuemy/incoh_census/fold_code/PSRCAT_PSR_2016.csv').readlines()
#data_PCAT=open('/group/mwaops/xuemy/incoh_census/fold_code/PSRCAT_PSR_DM200_2016.csv').readlines()
ra_PCAT=[]
dec_PCAT=[]
ra_PCAT_N=[]
dec_PCAT_N=[]

for line_PCAT in data_PCAT:
    if line_PCAT.startswith ('#'):
        continue
    words_PCAT=line_PCAT.split(',')

    if float(words_PCAT[3]) < 30.05:
        if float(words_PCAT[2]) > 180:
            ra_PCAT.append(-float(words_PCAT[2])/180.0*np.pi+2*np.pi)
        else:
            ra_PCAT.append(-float(words_PCAT[2])/180.0*np.pi)
        dec_PCAT.append(float(words_PCAT[3])/180.0*np.pi)
    else:
        if float(words_PCAT[2]) > 180:
            ra_PCAT_N.append(-float(words_PCAT[2])/180.0*np.pi+2*np.pi)
        else:
            ra_PCAT_N.append(-float(words_PCAT[2])/180.0*np.pi)
        dec_PCAT_N.append(float(words_PCAT[3])/180.0*np.pi)

"""
data_MWA=open('/group/mwaops/xuemy/incoh_census/fold_code/MWA_PSR_20170426.csv').readlines()
ra_MWA=[]
dec_MWA=[]

for line_MWA in data_MWA:
    if line_MWA.startswith ('#'):
        continue
    words_MWA=line_MWA.split(',')
    #print words_MWA[2]
    if float(words_MWA[2]) > 180:
        ra_MWA.append(-float(words_MWA[2])/180.0*np.pi+2*np.pi)
    else:
        ra_MWA.append(-float(words_MWA[2])/180.0*np.pi)
    dec_MWA.append(float(words_MWA[3])/180.0*np.pi)
"""

#levels = np.arange(0.2, 1., 0.2)
levels = np.arange(0.25, 1., 0.05)
colors=['0.25', '0.5', '0.5', '0.5', '0.5', '0.5', '0.5', '0.5', '0.5', '0.5', '0.5', '0.5', '0.5', '0.5']
linewidths=[0.7, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4]

txtfile=['1166459712,']
for line in txtfile:
    words=line.split(',')
    ob=words[0]
    if ob[0]!=('1'):
    	continue
    print "Obtaining metadata from http://mwa-metadata01.pawsey.org.au/metadata/ for OBS ID: "
    beam_meta_data = getmeta(service='obs', params={'obs_id':ob})
        
    ra = beam_meta_data[u'metadata'][u'ra_pointing']
    dec = beam_meta_data[u'metadata'][u'dec_pointing']
    time = beam_meta_data[u'stoptime'] - beam_meta_data[u'starttime'] #gps time
    delays = beam_meta_data[u'rfstreams'][u'0'][u'xdelays']

    minfreq = float(min(beam_meta_data[u'rfstreams'][u"0"][u'frequencies']))
    maxfreq = float(max(beam_meta_data[u'rfstreams'][u"0"][u'frequencies']))
    centrefreq = 1.28e6 * (minfreq + (maxfreq-minfreq)/2) #in MHz
    channels = beam_meta_data[u'rfstreams'][u"0"][u'frequencies']

    cord = [ob, ra, dec, time, delays,centrefreq, channels]

    RA=[] ; Dec=[] ; z=[] ; x=[] ; y=[]
    
    for i in range(-87,88,3):
        for j in range(0,361,3):
            Dec.append(i)
            RA.append(j)


    powout=get_beam_power(cord, zip(RA,Dec), dt=600)
    for i in range(len(RA)):
        temppower=powout[i,0,0]
        for t in range(0,(time+361)/720):
            if i%121 >= t:
                power_ra = powout[i-t,0,0] #ra+t*15./3600 3deg
            else : 
                power_ra = powout[i+121-t,0,0]
            if power_ra > temppower:
                temppower = power_ra
            #print temppower, power_ra
        z.append(temppower)
        if RA[i] > 180:
            x.append(-RA[i]/180.*np.pi+2*np.pi)
        else:
            x.append(-RA[i]/180.*np.pi)

        y.append(Dec[i]/180.*np.pi)
    plt.tricontour(x, y, z, levels=levels, alpha = 0.4, 
                   colors=colors,
                   linewidths=linewidths)

plt.colorbar(fraction=0.02, pad=0.03)

xtick_labels = ['10h', '8h', '6h', '4h', '2h', '0h', '22h', '20h', '18h', '16h', '14h']
ax.set_xticklabels(xtick_labels) 
ax.grid(True)

p1=ax.scatter(ra_PCAT_N, dec_PCAT_N, 1.5, lw=0, marker='o', color ='gray', label="Known pulsars beyond MWA Dec limitation")
p1=ax.scatter(ra_PCAT, dec_PCAT, 1.5, lw=0, marker='o', color ='blue', label="Known pulsars MWA could reach")


#handles, labels = ax.get_legend_handles_labels()
plt.legend(bbox_to_anchor=(0.65, 1.02,0.4,0.2), loc=3,numpoints=1,
           ncol=1, mode="expand", borderaxespad=0., fontsize=6)


plt.savefig('all_contour_archive_201706_test.png', figsize=(5,3), dpi=600)
plt.show()
